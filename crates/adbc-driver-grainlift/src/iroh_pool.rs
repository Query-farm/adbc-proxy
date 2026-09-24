// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Process-wide Iroh resources with one independent VGI stream per caller.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use vgi_rpc_client::RpcClient;
use vgi_rpc_iroh::{IrohClientOptions, IrohConnection};

#[derive(Clone, Debug)]
pub(crate) struct PoolError {
    context: &'static str,
    message: String,
}

impl PoolError {
    fn new(context: &'static str, error: impl fmt::Display) -> Self {
        Self {
            context,
            message: error.to_string(),
        }
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, self.message)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PoolKey {
    local_id: EndpointId,
    remote_id: EndpointId,
    direct_address: Option<SocketAddr>,
    rpc_timeout: Duration,
}

pub(crate) struct Config {
    pub remote_id: EndpointId,
    pub direct_address: Option<SocketAddr>,
    pub secret_key: Option<SecretKey>,
    pub rpc_timeout: Duration,
}

/// Keeps the physical connection alive while a caller owns its logical stream.
pub(crate) struct Lease {
    _slot: Arc<Slot<ConnectionResource>>,
}

pub(crate) struct PooledClient {
    client: RpcClient,
    lease: Lease,
}

impl PooledClient {
    pub(crate) fn into_parts(self) -> (RpcClient, Lease) {
        (self.client, self.lease)
    }
}

struct ConnectionResource {
    _endpoint: Endpoint,
    connection: IrohConnection,
}

impl Drop for ConnectionResource {
    fn drop(&mut self) {
        self.connection.close();
    }
}

type InitResult<V> = Result<Arc<V>, PoolError>;

struct Slot<V> {
    value: OnceLock<InitResult<V>>,
}

impl<V> Default for Slot<V> {
    fn default() -> Self {
        Self {
            value: OnceLock::new(),
        }
    }
}

struct WeakPool<K, V> {
    entries: Mutex<HashMap<K, Weak<Slot<V>>>>,
}

impl<K, V> Default for WeakPool<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V> WeakPool<K, V>
where
    K: Clone + Eq + Hash,
{
    fn acquire(
        &self,
        key: K,
        initialize: impl FnOnce() -> InitResult<V>,
    ) -> Result<(Arc<Slot<V>>, Arc<V>), PoolError> {
        let slot = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| PoolError::new("lock Iroh pool", "pool is poisoned"))?;
            entries.retain(|_, slot| slot.strong_count() > 0);
            match entries.get(&key).and_then(Weak::upgrade) {
                Some(slot) => slot,
                None => {
                    let slot = Arc::new(Slot::default());
                    entries.insert(key, Arc::downgrade(&slot));
                    slot
                }
            }
        };
        let resource = slot.value.get_or_init(initialize).clone()?;
        Ok((slot, resource))
    }

    fn invalidate(&self, key: &K, expected: &Arc<Slot<V>>) {
        if let Ok(mut entries) = self.entries.lock()
            && entries
                .get(key)
                .and_then(Weak::upgrade)
                .is_some_and(|slot| Arc::ptr_eq(&slot, expected))
        {
            entries.remove(key);
        }
    }

    #[cfg(test)]
    fn live_entries(&self) -> usize {
        self.entries
            .lock()
            .expect("pool lock")
            .values()
            .filter(|entry| entry.strong_count() > 0)
            .count()
    }
}

fn runtime() -> Result<&'static tokio::runtime::Runtime, PoolError> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, PoolError>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("grainlift-iroh")
                .enable_all()
                .build()
                .map_err(|error| PoolError::new("create Iroh runtime", error))
        })
        .as_ref()
        .map_err(Clone::clone)
}

fn process_secret_key() -> SecretKey {
    static SECRET: OnceLock<SecretKey> = OnceLock::new();
    SECRET.get_or_init(SecretKey::generate).clone()
}

fn pool() -> &'static WeakPool<PoolKey, ConnectionResource> {
    static POOL: OnceLock<WeakPool<PoolKey, ConnectionResource>> = OnceLock::new();
    POOL.get_or_init(WeakPool::default)
}

fn initialize_resource(
    secret_key: SecretKey,
    remote_id: EndpointId,
    direct_address: Option<SocketAddr>,
    rpc_timeout: Duration,
) -> InitResult<ConnectionResource> {
    runtime()?.block_on(async move {
        let endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|error| PoolError::new("bind Iroh client endpoint", error))?;
        let mut remote = EndpointAddr::new(remote_id);
        if let Some(address) = direct_address {
            remote = remote.with_ip_addr(address);
        }
        let connection = IrohConnection::connect_addr(
            endpoint.clone(),
            remote,
            IrohClientOptions::default().with_rpc_timeout(rpc_timeout),
        )
        .await
        .map_err(|error| PoolError::new("connect Iroh endpoint", error))?;
        Ok(Arc::new(ConnectionResource {
            _endpoint: endpoint,
            connection,
        }))
    })
}

/// Acquire a shared physical connection and open a fresh logical VGI stream.
///
/// A failed stream open evicts that physical connection and retries once. This
/// prevents a connection that a peer has closed from poisoning future callers.
pub(crate) fn open_client(config: Config) -> Result<PooledClient, PoolError> {
    let secret_key = config.secret_key.unwrap_or_else(process_secret_key);
    let key = PoolKey {
        local_id: secret_key.public(),
        remote_id: config.remote_id,
        direct_address: config.direct_address,
        rpc_timeout: config.rpc_timeout,
    };

    for attempt in 0..2 {
        let initialization_secret = secret_key.clone();
        let (slot, resource) = pool().acquire(key.clone(), || {
            initialize_resource(
                initialization_secret,
                key.remote_id,
                key.direct_address,
                key.rpc_timeout,
            )
        })?;
        match runtime()?.block_on(resource.connection.open_client()) {
            Ok(client) => {
                return Ok(PooledClient {
                    client,
                    lease: Lease { _slot: slot },
                });
            }
            Err(error) if attempt == 0 => {
                pool().invalidate(&key, &slot);
                drop(resource);
                drop(slot);
                let _ = error;
            }
            Err(error) => return Err(PoolError::new("open Iroh VGI stream", error)),
        }
    }
    unreachable!("Iroh stream open retry loop has two exhaustive attempts")
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use super::*;

    #[test]
    fn same_key_reuses_one_resource_without_a_stampede() {
        let pool = Arc::new(WeakPool::<u8, usize>::default());
        let starts = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let pool = pool.clone();
                let starts = starts.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let (slot, resource) = pool
                        .acquire(7, || {
                            starts.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(10));
                            Ok(Arc::new(42))
                        })
                        .unwrap();
                    (slot, resource)
                })
            })
            .collect::<Vec<_>>();
        let resources = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert!(
            resources
                .iter()
                .all(|(_, resource)| Arc::ptr_eq(resource, &resources[0].1))
        );
        assert_eq!(pool.live_entries(), 1);
        drop(resources);
        let (_slot, _resource) = pool
            .acquire(8, || Ok(Arc::new(43)))
            .expect("sweep dead entry");
        assert_eq!(pool.live_entries(), 1);
    }

    #[test]
    fn distinct_keys_are_isolated_and_dead_slots_are_swept() {
        let pool = WeakPool::<u8, usize>::default();
        let (first_slot, first) = pool
            .acquire(1, || Ok(Arc::new(10)))
            .expect("first resource");
        let (_second_slot, second) = pool
            .acquire(2, || Ok(Arc::new(20)))
            .expect("second resource");
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(pool.live_entries(), 2);
        drop(first);
        drop(first_slot);
        let (_third_slot, _third) = pool
            .acquire(3, || Ok(Arc::new(30)))
            .expect("third resource");
        assert_eq!(pool.live_entries(), 2);
    }

    #[test]
    fn failed_initialization_does_not_permanently_poison_a_key() {
        let pool = WeakPool::<u8, usize>::default();
        let error = pool
            .acquire(1, || Err(PoolError::new("initialize", "failed")))
            .err()
            .expect("first initialization must fail");
        assert_eq!(error.to_string(), "initialize: failed");
        let (_slot, resource) = pool
            .acquire(1, || Ok(Arc::new(42)))
            .expect("a later acquisition may retry");
        assert_eq!(*resource, 42);
    }

    #[test]
    fn connection_key_includes_identity_route_and_timeout() {
        let first_local = SecretKey::generate().public();
        let second_local = SecretKey::generate().public();
        let remote = SecretKey::generate().public();
        let base = PoolKey {
            local_id: first_local,
            remote_id: remote,
            direct_address: Some("127.0.0.1:9000".parse().unwrap()),
            rpc_timeout: Duration::from_secs(1),
        };
        let mut changed = base.clone();
        changed.local_id = second_local;
        assert_ne!(base, changed);
        changed = base.clone();
        changed.direct_address = Some("127.0.0.1:9001".parse().unwrap());
        assert_ne!(base, changed);
        changed = base.clone();
        changed.rpc_timeout = Duration::from_secs(2);
        assert_ne!(base, changed);
    }
}
