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

use adbc_core::error::{Error, Status};
use grainlift_protocol::{Bytes, PartitionClaims, decode_record_ipc, encode_record_ipc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct PartitionSigner {
    key: [u8; 32],
    ttl: Duration,
}

fn invalid() -> Error {
    Error::with_message_and_status(
        "Partition descriptor is invalid, expired, or unavailable to this principal",
        Status::NotFound,
    )
}
fn now_ms() -> Result<i64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or_else(invalid)
}

impl PartitionSigner {
    pub fn new(ttl: Duration) -> Self {
        Self {
            key: rand::random(),
            ttl,
        }
    }

    fn mac(&self, bytes: &[u8]) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts a 32-byte key");
        mac.update(bytes);
        mac
    }

    fn owner(&self, target: &str, principal: &str) -> String {
        let mut mac = self.mac(target.as_bytes());
        mac.update(b"\0");
        mac.update(principal.as_bytes());
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub fn seal(
        &self,
        target: &str,
        principal: &str,
        descriptor: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        let lifetime = i64::try_from(self.ttl.as_millis()).map_err(|_| invalid())?;
        let expires_at_ms = now_ms()?.checked_add(lifetime).ok_or_else(invalid)?;
        let claims = PartitionClaims {
            version: 1,
            expires_at_ms,
            owner: self.owner(target, principal),
            descriptor: Bytes(descriptor),
        };
        let payload = encode_record_ipc(claims, limit.saturating_sub(36)).map_err(|_| invalid())?;
        let mut token = Vec::with_capacity(36 + payload.len());
        token.extend_from_slice(b"GLP2");
        token.extend_from_slice(&self.mac(&payload).finalize().into_bytes());
        token.extend_from_slice(&payload);
        Ok(token)
    }

    pub fn open(
        &self,
        target: &str,
        principal: &str,
        token: &[u8],
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        if token.len() < 36 || token.len() > limit || &token[..4] != b"GLP2" {
            return Err(invalid());
        }
        self.mac(&token[36..])
            .verify_slice(&token[4..36])
            .map_err(|_| invalid())?;
        let claims: PartitionClaims =
            decode_record_ipc(&token[36..], limit - 36).map_err(|_| invalid())?;
        if claims.version != 1
            || claims.expires_at_ms <= now_ms()?
            || claims.owner != self.owner(target, principal)
        {
            return Err(invalid());
        }
        Ok(claims.descriptor.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_partitions_are_scoped_expiring_and_process_local() {
        let signer = PartitionSigner::new(Duration::from_secs(60));
        let token = signer.seal("target", "alice", vec![0, 255], 65536).unwrap();
        assert_eq!(
            signer.open("target", "alice", &token, 65536).unwrap(),
            [0, 255]
        );
        assert!(signer.open("target", "bob", &token, 65536).is_err());
        assert!(signer.open("other", "alice", &token, 65536).is_err());
        assert!(
            PartitionSigner::new(Duration::from_secs(60))
                .open("target", "alice", &token, 65536)
                .is_err()
        );
        let mut tampered = token.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(signer.open("target", "alice", &tampered, 65536).is_err());
        assert!(
            signer
                .open("target", "alice", b"GLP1legacy", 65536)
                .is_err()
        );
        let expired = PartitionSigner::new(Duration::ZERO);
        let token = expired.seal("target", "alice", vec![], 65536).unwrap();
        assert!(expired.open("target", "alice", &token, 65536).is_err());
    }

    #[test]
    fn partition_bounds_and_authenticated_claim_validation() {
        let signer = PartitionSigner::new(Duration::from_secs(60));
        let token = signer.seal("target", "alice", vec![1], 65536).unwrap();
        assert!(
            signer
                .open("target", "alice", &token, token.len() - 1)
                .is_err()
        );
        for limit in [token.len(), token.len() + 1] {
            assert_eq!(signer.open("target", "alice", &token, limit).unwrap(), [1]);
        }
        assert!(signer.seal("target", "alice", vec![1], 1).is_err());
        // Even an authenticated payload must have the exact claims schema.
        let payload =
            encode_record_ipc(grainlift_protocol::OkResponse { ok: true }, 65536).unwrap();
        let mut forged = b"GLP2".to_vec();
        forged.extend_from_slice(&signer.mac(&payload).finalize().into_bytes());
        forged.extend_from_slice(&payload);
        assert!(signer.open("target", "alice", &forged, 65536).is_err());
    }
}
