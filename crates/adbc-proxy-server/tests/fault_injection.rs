use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
use adbc_core::{CancelHandle, PartitionedResult};
use adbc_proxy_protocol as protocol;
use adbc_proxy_server::backend::{Backend, BackendConnection, BackendStatement};
use adbc_proxy_server::config::TargetConfig;
use adbc_proxy_server::service::build_server;
use adbc_proxy_server::session::{ResourceCounts, SessionLimits, SessionManager, TargetAuthorizer};
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use vgi_rpc::AuthContext;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::http::HttpState;
use vgi_rpc_client::http::{HttpClient, HttpClientBuilder};

#[derive(Default)]
struct FaultState {
    connection_cancels: AtomicUsize,
    statement_cancels: AtomicUsize,
    connection_drops: AtomicUsize,
    statement_drops: AtomicUsize,
    reader_drops: AtomicUsize,
    reader_nexts: AtomicUsize,
    commit_delay_ms: AtomicU64,
    commit_started: AtomicBool,
    commit_finished: AtomicBool,
    connection_cancel_error: Mutex<Option<AdbcError>>,
    statement_cancel_error: Mutex<Option<AdbcError>>,
    execute_error: Mutex<Option<AdbcError>>,
}

struct FaultBackend {
    state: Arc<FaultState>,
}

struct FaultConnection {
    state: Arc<FaultState>,
}

struct FaultStatement {
    state: Arc<FaultState>,
}

enum CancelKind {
    Connection,
    Statement,
}

struct FaultCancel {
    state: Arc<FaultState>,
    kind: CancelKind,
}

impl Backend for FaultBackend {
    fn open(
        &self,
        _target: &TargetConfig,
        _database_options: Vec<(String, OptionValue)>,
        _connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>> {
        Ok(Box::new(FaultConnection {
            state: Arc::clone(&self.state),
        }))
    }
}

impl BackendConnection for FaultConnection {
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(FaultCancel {
            state: Arc::clone(&self.state),
            kind: CancelKind::Connection,
        })
    }

    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>> {
        Ok(Box::new(FaultStatement {
            state: Arc::clone(&self.state),
        }))
    }

    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        unsupported()
    }

    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        unsupported()
    }

    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        unsupported()
    }

    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        unsupported()
    }

    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        unsupported()
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> AdbcResult<Schema> {
        unsupported()
    }

    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }

    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }

    fn commit(&mut self) -> AdbcResult<()> {
        self.state.commit_started.store(true, Ordering::SeqCst);
        let delay = self.state.commit_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        self.state.commit_finished.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    fn read_partition(
        &self,
        _partition: &[u8],
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        unsupported()
    }
}

impl Drop for FaultConnection {
    fn drop(&mut self) {
        self.state.connection_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl BackendStatement for FaultStatement {
    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        unsupported()
    }

    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        unsupported()
    }

    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        unsupported()
    }

    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        unsupported()
    }

    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        unsupported()
    }

    fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
        unsupported()
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        unsupported()
    }

    fn set_sql_query(&mut self, _query: &str) -> AdbcResult<()> {
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        unsupported()
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        if let Some(error) = self.state.execute_error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(Box::new(FaultReader::batches(
            Arc::clone(&self.state),
            [1, 2],
        )))
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        unsupported()
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        unsupported()
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        unsupported()
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        unsupported()
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(FaultCancel {
            state: Arc::clone(&self.state),
            kind: CancelKind::Statement,
        })
    }
}

impl Drop for FaultStatement {
    fn drop(&mut self) {
        self.state.statement_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl CancelHandle for FaultCancel {
    fn try_cancel(&self) -> AdbcResult<()> {
        let error = match self.kind {
            CancelKind::Connection => {
                self.state.connection_cancels.fetch_add(1, Ordering::SeqCst);
                self.state.connection_cancel_error.lock().unwrap().clone()
            }
            CancelKind::Statement => {
                self.state.statement_cancels.fetch_add(1, Ordering::SeqCst);
                self.state.statement_cancel_error.lock().unwrap().clone()
            }
        };
        error.map_or(Ok(()), Err)
    }
}

enum ReaderScript {
    Batches(VecDeque<RecordBatch>),
    ErrorThenBatch {
        emitted_error: bool,
        batch: Option<RecordBatch>,
    },
}

struct FaultReader {
    state: Arc<FaultState>,
    schema: SchemaRef,
    script: ReaderScript,
}

impl FaultReader {
    fn batches(state: Arc<FaultState>, values: impl IntoIterator<Item = i64>) -> Self {
        let schema = value_schema();
        let batches = values
            .into_iter()
            .map(|value| value_batch(Arc::clone(&schema), value))
            .collect();
        Self {
            state,
            schema,
            script: ReaderScript::Batches(batches),
        }
    }

    fn error_then_batch(state: Arc<FaultState>) -> Self {
        let schema = value_schema();
        Self {
            state,
            schema: Arc::clone(&schema),
            script: ReaderScript::ErrorThenBatch {
                emitted_error: false,
                batch: Some(value_batch(schema, 99)),
            },
        }
    }
}

impl Iterator for FaultReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.reader_nexts.fetch_add(1, Ordering::SeqCst);
        match &mut self.script {
            ReaderScript::Batches(batches) => batches.pop_front().map(Ok),
            ReaderScript::ErrorThenBatch {
                emitted_error,
                batch,
            } => {
                if !*emitted_error {
                    *emitted_error = true;
                    Some(Err(ArrowError::ComputeError(
                        "deterministic downstream reader failure".to_string(),
                    )))
                } else {
                    batch.take().map(Ok)
                }
            }
        }
    }
}

impl RecordBatchReader for FaultReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for FaultReader {
    fn drop(&mut self) {
        self.state.reader_drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn unsupported<T>() -> AdbcResult<T> {
    Err(AdbcError::with_message_and_status(
        "unused fault-injection operation",
        Status::NotImplemented,
    ))
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(predicate(), "condition did not become true before deadline");
}

fn structured_error(status: Status, message: &str) -> AdbcError {
    AdbcError {
        message: message.to_string(),
        status,
        vendor_code: 8675,
        sqlstate: [
            b'H' as std::os::raw::c_char,
            b'Y' as std::os::raw::c_char,
            b'0' as std::os::raw::c_char,
            b'0' as std::os::raw::c_char,
            b'8' as std::os::raw::c_char,
        ],
        details: Some(vec![("fault.kind".to_string(), b"injected".to_vec())]),
    }
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn value_batch(schema: SchemaRef, value: i64) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

fn target() -> TargetConfig {
    TargetConfig {
        driver: "fault-driver".to_string(),
        entrypoint: None,
        database_options: Vec::new(),
        connection_options: Vec::new(),
        allow_client_database_options: false,
        allow_client_connection_options: false,
    }
}

fn manager(
    state: Arc<FaultState>,
    ttl: Duration,
    require_authentication: bool,
) -> Arc<SessionManager> {
    Arc::new(SessionManager::with_limits_and_authorizer(
        Arc::new(FaultBackend { state }),
        HashMap::from([("fault".to_string(), target())]),
        ttl,
        require_authentication,
        SessionLimits {
            max_sessions: 4,
            max_sessions_per_principal: 2,
            max_statements_per_session: 2,
            max_results_per_session: 2,
        },
        TargetAuthorizer::default(),
    ))
}

fn manager_with_operation_timeout(
    state: Arc<FaultState>,
    operation_timeout: Duration,
) -> Arc<SessionManager> {
    Arc::new(SessionManager::with_limits_authorizer_and_timeout(
        Arc::new(FaultBackend { state }),
        HashMap::from([("fault".to_string(), target())]),
        Duration::from_secs(60),
        false,
        SessionLimits {
            max_sessions: 4,
            max_sessions_per_principal: 2,
            max_statements_per_session: 2,
            max_results_per_session: 2,
        },
        TargetAuthorizer::default(),
        operation_timeout,
    ))
}

fn open_session(manager: &SessionManager, principal: &str) -> (String, String) {
    let session_id = manager
        .open(principal.to_string(), "fault", Vec::new(), Vec::new())
        .unwrap();
    let session = manager.get(&session_id, principal).unwrap();
    let statement_id = session.new_statement().unwrap();
    (session_id, statement_id)
}

fn session_request(session_id: &str) -> RecordBatch {
    protocol::one_string(protocol::session_schema(), session_id).unwrap()
}

fn statement_request(session_id: &str, statement_id: &str) -> RecordBatch {
    RecordBatch::try_new(
        protocol::statement_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id])),
            Arc::new(StringArray::from(vec![statement_id])),
        ],
    )
    .unwrap()
}

fn result_request(session_id: &str, result_id: &str, sequence: i64) -> RecordBatch {
    RecordBatch::try_new(
        protocol::read_result_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id])),
            Arc::new(StringArray::from(vec![result_id])),
            Arc::new(Int64Array::from(vec![sequence])),
        ],
    )
    .unwrap()
}

fn client(endpoint: &str, token: Option<&str>) -> HttpClient {
    client_with_timeout(endpoint, token, Duration::from_secs(2))
}

fn client_with_timeout(endpoint: &str, token: Option<&str>, timeout: Duration) -> HttpClient {
    let mut builder: HttpClientBuilder = HttpClient::connect(endpoint.to_string())
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .timeout(Some(timeout));
    if let Some(token) = token {
        builder = builder
            .header("authorization", &format!("Bearer {token}"))
            .unwrap();
    }
    builder.build().unwrap()
}

async fn start_server(
    manager: Arc<SessionManager>,
    timeout: Duration,
    authenticated: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    let server = Arc::new(build_server(manager, "fault-worker".to_string()));
    let mut builder = HttpState::builder().server(server).request_timeout(timeout);
    if authenticated {
        builder = builder.authenticate(bearer_authenticate_static(HashMap::from([
            (
                "alice-token".to_string(),
                AuthContext::for_principal("test", "alice"),
            ),
            (
                "bob-token".to_string(),
                AuthContext::for_principal("test", "bob"),
            ),
        ])));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(builder.build()))
            .await
            .unwrap();
    });
    (format!("http://{address}"), task)
}

fn wire_error(error: vgi_rpc::RpcError) -> protocol::WireAdbcError {
    assert_eq!(error.error_type, "AdbcError", "unexpected error: {error}");
    serde_json::from_str(&error.message).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_cancel_reclaims_only_the_result_and_abandonment_uses_lease_cleanup() {
    let state = Arc::new(FaultState::default());
    let manager = manager(Arc::clone(&state), Duration::from_millis(30), false);
    let (session_id, statement_id) = open_session(&manager, "\0anonymous");
    let session = manager.get(&session_id, "\0anonymous").unwrap();
    let (result_id, _) = session
        .insert_statement_result(
            &statement_id,
            Box::new(FaultReader::batches(Arc::clone(&state), [10, 11])),
        )
        .unwrap();
    drop(session);
    let (endpoint, server) =
        start_server(Arc::clone(&manager), Duration::from_secs(1), false).await;

    let request = result_request(&session_id, &result_id, 0);
    tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        move || {
            let mut client = client(&endpoint, None);
            let mut stream = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .unwrap();
            assert!(stream.next_with_token().unwrap().is_some());
            stream.cancel().unwrap();
        }
    })
    .await
    .unwrap();

    assert_eq!(manager.resource_counts().unwrap().results, 0);
    assert_eq!(state.reader_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.statement_cancels.load(Ordering::SeqCst), 0);
    assert_eq!(state.connection_cancels.load(Ordering::SeqCst), 0);

    let session = manager.get(&session_id, "\0anonymous").unwrap();
    let (abandoned_id, _) = session
        .insert_statement_result(
            &statement_id,
            Box::new(FaultReader::batches(Arc::clone(&state), [20, 21])),
        )
        .unwrap();
    drop(session);
    let request = result_request(&session_id, &abandoned_id, 0);
    tokio::task::spawn_blocking(move || {
        let mut client = client(&endpoint, None);
        let mut stream = client
            .open_producer(protocol::method::READ_RESULT, &request, None, false)
            .unwrap();
        assert!(stream.next_with_token().unwrap().is_some());
        // Dropping without a cancellation exchange models an abrupt caller or
        // peer disappearance. Logical sessions are reconnectable, so cleanup
        // intentionally happens through the bounded lease instead.
        drop(stream);
    })
    .await
    .unwrap();
    assert_eq!(manager.resource_counts().unwrap().results, 1);
    tokio::time::sleep(Duration::from_millis(45)).await;
    assert_eq!(manager.reap_expired().unwrap(), 1);
    assert_eq!(
        manager.resource_counts().unwrap(),
        ResourceCounts::default()
    );
    wait_until(|| state.reader_drops.load(Ordering::SeqCst) == 2);
    assert_eq!(state.reader_drops.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adbc_cancellation_is_separate_authorized_and_preserves_unsupported_errors() {
    let state = Arc::new(FaultState::default());
    let manager = manager(Arc::clone(&state), Duration::from_secs(60), true);
    let (session_id, statement_id) = open_session(&manager, "test\0alice");
    let (endpoint, server) = start_server(Arc::clone(&manager), Duration::from_secs(1), true).await;

    let statement = statement_request(&session_id, &statement_id);
    let session = session_request(&session_id);
    tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        move || {
            let mut alice = client(&endpoint, Some("alice-token"));
            alice
                .call_unary(protocol::method::CANCEL_STATEMENT, &statement, None)
                .unwrap();
            alice
                .call_unary(protocol::method::CANCEL_CONNECTION, &session, None)
                .unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(state.statement_cancels.load(Ordering::SeqCst), 1);
    assert_eq!(state.connection_cancels.load(Ordering::SeqCst), 1);

    *state.statement_cancel_error.lock().unwrap() = Some(structured_error(
        Status::NotImplemented,
        "downstream statement cancellation is unsupported",
    ));
    let error = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let statement = statement_request(&session_id, &statement_id);
        move || {
            client(&endpoint, Some("alice-token"))
                .call_unary(protocol::method::CANCEL_STATEMENT, &statement, None)
                .unwrap_err()
        }
    })
    .await
    .unwrap();
    let wire = wire_error(error);
    assert_eq!(wire.status, "not_implemented");
    assert_eq!(wire.vendor_code, 8675);
    assert_eq!(wire.sqlstate, vec![72, 89, 48, 48, 56]);
    assert!(!wire.details.is_empty());

    let error = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let statement = statement_request(&session_id, &statement_id);
        move || {
            client(&endpoint, Some("bob-token"))
                .call_unary(protocol::method::CANCEL_STATEMENT, &statement, None)
                .unwrap_err()
        }
    })
    .await
    .unwrap();
    assert_eq!(wire_error(error).status, "unauthorized");
    assert_eq!(state.statement_cancels.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_and_replayed_reads_are_stable_and_service_remains_healthy() {
    let state = Arc::new(FaultState::default());
    let manager = manager(Arc::clone(&state), Duration::from_secs(60), false);
    let (session_id, statement_id) = open_session(&manager, "\0anonymous");
    let session = manager.get(&session_id, "\0anonymous").unwrap();
    let (result_id, _) = session
        .insert_statement_result(
            &statement_id,
            Box::new(FaultReader::batches(Arc::clone(&state), [7, 8])),
        )
        .unwrap();
    drop(session);
    let (endpoint, server) =
        start_server(Arc::clone(&manager), Duration::from_secs(1), false).await;

    let (first_value, replay_value, gap_status) = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let session_id = session_id.clone();
        let result_id = result_id.clone();
        move || {
            let mut client = client(&endpoint, None);
            let request = result_request(&session_id, &result_id, 0);
            let first = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .unwrap()
                .next_with_token()
                .unwrap()
                .unwrap()
                .0
                .0;
            let replay = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .unwrap()
                .next_with_token()
                .unwrap()
                .unwrap()
                .0
                .0;
            let gap = result_request(&session_id, &result_id, 2);
            let gap = client
                .open_producer(protocol::method::READ_RESULT, &gap, None, false)
                .err()
                .expect("gap read must fail");
            let next = result_request(&session_id, &result_id, 1);
            client
                .open_producer(protocol::method::READ_RESULT, &next, None, false)
                .unwrap()
                .next_with_token()
                .unwrap();
            (
                protocol::int64_value(&first, "value").unwrap(),
                protocol::int64_value(&replay, "value").unwrap(),
                wire_error(gap).status,
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(first_value, replay_value);
    assert_eq!(gap_status, "invalid_state");
    assert_eq!(state.reader_nexts.load(Ordering::SeqCst), 2);

    let session = manager.get(&session_id, "\0anonymous").unwrap();
    let (error_result_id, _) = session
        .insert_statement_result(
            &statement_id,
            Box::new(FaultReader::error_then_batch(Arc::clone(&state))),
        )
        .unwrap();
    drop(session);
    let before = state.reader_nexts.load(Ordering::SeqCst);
    let (first_error, replay_error, malformed_type) = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let session_id = session_id.clone();
        move || {
            let mut client = client(&endpoint, None);
            let request = result_request(&session_id, &error_result_id, 0);
            let first = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .err()
                .expect("reader error must be surfaced");
            let replay = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .err()
                .expect("reader error replay must be surfaced");
            let malformed = session_request(&session_id);
            let malformed = client
                .call_unary(protocol::method::CANCEL_STATEMENT, &malformed, None)
                .unwrap_err();
            client
                .call_unary(
                    protocol::method::ROLLBACK,
                    &session_request(&session_id),
                    None,
                )
                .unwrap();
            (wire_error(first), wire_error(replay), malformed.error_type)
        }
    })
    .await
    .unwrap();
    assert_eq!(first_error, replay_error);
    assert_eq!(state.reader_nexts.load(Ordering::SeqCst), before + 1);
    assert_ne!(malformed_type, "AdbcError");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_actor_deadline_and_cancellation_are_independent() {
    let state = Arc::new(FaultState::default());
    state.commit_delay_ms.store(250, Ordering::SeqCst);
    let manager = manager_with_operation_timeout(Arc::clone(&state), Duration::from_millis(30));
    let (session_id, statement_id) = open_session(&manager, "\0anonymous");
    let session = manager.get(&session_id, "\0anonymous").unwrap();

    let started = std::time::Instant::now();
    let error = session.commit().unwrap_err();
    assert_eq!(error.status, Status::Timeout);
    assert!(started.elapsed() < Duration::from_millis(150));
    assert!(state.commit_started.load(Ordering::SeqCst));
    assert!(!state.commit_finished.load(Ordering::SeqCst));

    let mut cancel_threads = Vec::new();
    for _ in 0..8 {
        let session = Arc::clone(&session);
        let statement_id = statement_id.clone();
        cancel_threads.push(std::thread::spawn(move || {
            session.cancel_connection().unwrap();
            session.cancel_statement(&statement_id).unwrap();
        }));
    }
    for thread in cancel_threads {
        thread.join().unwrap();
    }
    assert_eq!(state.connection_cancels.load(Ordering::SeqCst), 8);
    assert_eq!(state.statement_cancels.load(Ordering::SeqCst), 8);

    let queued = session.rollback().unwrap_err();
    assert_eq!(queued.status, Status::Timeout);
    tokio::time::sleep(Duration::from_millis(260)).await;
    session.rollback().unwrap();
}

#[test]
fn shutdown_detaches_a_timed_out_driver_without_joining_its_worker() {
    let state = Arc::new(FaultState::default());
    state.commit_delay_ms.store(500, Ordering::SeqCst);
    let manager = manager_with_operation_timeout(Arc::clone(&state), Duration::from_millis(20));
    let (session_id, _) = open_session(&manager, "\0anonymous");
    let session = manager.get(&session_id, "\0anonymous").unwrap();
    assert_eq!(session.commit().unwrap_err().status, Status::Timeout);
    assert!(state.commit_started.load(Ordering::SeqCst));

    let started = std::time::Instant::now();
    assert_eq!(manager.close_all().unwrap(), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(manager.resource_counts().unwrap().sessions, 0);
    drop(session);
    wait_until(|| state.connection_cancels.load(Ordering::SeqCst) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_deadline_is_a_structured_adbc_timeout_not_a_transport_timeout() {
    let state = Arc::new(FaultState::default());
    state.commit_delay_ms.store(200, Ordering::SeqCst);
    let manager = manager_with_operation_timeout(Arc::clone(&state), Duration::from_millis(20));
    let (session_id, _) = open_session(&manager, "\0anonymous");
    let (endpoint, server) =
        start_server(Arc::clone(&manager), Duration::from_secs(1), false).await;
    let wire = tokio::task::spawn_blocking(move || {
        let error = client(&endpoint, None)
            .call_unary(
                protocol::method::COMMIT,
                &session_request(&session_id),
                None,
            )
            .unwrap_err();
        wire_error(error)
    })
    .await
    .unwrap();
    assert_eq!(wire.status, "timeout");
    assert_eq!(state.connection_cancels.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_request_timeout_does_not_implicitly_cancel_adbc_or_poison_the_session() {
    let state = Arc::new(FaultState::default());
    state.commit_delay_ms.store(100, Ordering::SeqCst);
    let manager = manager(Arc::clone(&state), Duration::from_secs(60), false);
    let (session_id, _) = open_session(&manager, "\0anonymous");
    let (endpoint, server) =
        start_server(Arc::clone(&manager), Duration::from_secs(1), false).await;

    let timed_out = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let request = session_request(&session_id);
        move || {
            client_with_timeout(&endpoint, None, Duration::from_millis(20))
                .call_unary(protocol::method::COMMIT, &request, None)
                .unwrap_err()
        }
    })
    .await
    .unwrap();
    assert!(
        timed_out.error_type == "TransportError"
            || timed_out.error_type.contains("Http")
            || timed_out.error_type.contains("Timeout")
            || timed_out.message.contains("timed out"),
        "unexpected timeout error: {timed_out}"
    );
    assert_eq!(state.connection_cancels.load(Ordering::SeqCst), 0);
    assert_eq!(state.statement_cancels.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(120)).await;
    state.commit_delay_ms.store(0, Ordering::SeqCst);
    tokio::task::spawn_blocking({
        let request = session_request(&session_id);
        move || {
            client(&endpoint, None)
                .call_unary(protocol::method::ROLLBACK, &request, None)
                .unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.resource_counts().unwrap().sessions, 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_reaper_does_not_expire_an_in_flight_database_operation() {
    let state = Arc::new(FaultState::default());
    state.commit_delay_ms.store(700, Ordering::SeqCst);
    let manager = manager(Arc::clone(&state), Duration::from_millis(500), false);
    let (session_id, _) = open_session(&manager, "\0anonymous");
    let (endpoint, server) =
        start_server(Arc::clone(&manager), Duration::from_secs(2), false).await;
    let request = session_request(&session_id);
    let call = tokio::task::spawn_blocking(move || {
        client(&endpoint, None)
            .call_unary(protocol::method::COMMIT, &request, None)
            .unwrap();
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while !state.commit_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    assert_eq!(manager.reap_expired().unwrap(), 0);
    assert_eq!(manager.resource_counts().unwrap().sessions, 1);

    call.await.unwrap();
    assert!(state.commit_finished.load(Ordering::SeqCst));
    assert_eq!(manager.reap_expired().unwrap(), 1);
    assert_eq!(
        manager.resource_counts().unwrap(),
        ResourceCounts::default()
    );
    server.abort();
}

#[test]
fn shutdown_detaches_live_handles_and_drops_them_after_in_flight_users_finish() {
    let state = Arc::new(FaultState::default());
    let manager = manager(Arc::clone(&state), Duration::from_secs(60), false);
    let (session_id, statement_id) = open_session(&manager, "\0anonymous");
    let in_flight = manager.get(&session_id, "\0anonymous").unwrap();
    in_flight
        .insert_statement_result(
            &statement_id,
            Box::new(FaultReader::batches(Arc::clone(&state), [1, 2])),
        )
        .unwrap();
    assert_eq!(
        manager.resource_counts().unwrap(),
        ResourceCounts {
            sessions: 1,
            statements: 1,
            results: 1,
            bind_uploads: 0,
            opening_sessions: 0,
        }
    );

    assert_eq!(manager.close_all().unwrap(), 1);
    assert_eq!(
        manager.resource_counts().unwrap(),
        ResourceCounts::default()
    );
    assert_eq!(state.connection_drops.load(Ordering::SeqCst), 0);
    assert!(matches!(
        manager.get(&session_id, "\0anonymous"),
        Err(AdbcError {
            status: Status::NotFound,
            ..
        })
    ));

    drop(in_flight);
    wait_until(|| state.connection_drops.load(Ordering::SeqCst) == 1);
    assert_eq!(state.reader_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.statement_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.connection_drops.load(Ordering::SeqCst), 1);

    // Shutdown is terminal: a late connection cannot repopulate the detached
    // registry.
    assert_eq!(
        manager
            .open("\0anonymous".to_string(), "fault", Vec::new(), Vec::new())
            .unwrap_err()
            .status,
        Status::InvalidState
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downstream_execute_error_keeps_all_adbc_error_fields() {
    let state = Arc::new(FaultState::default());
    *state.execute_error.lock().unwrap() = Some(structured_error(
        Status::Cancelled,
        "deterministic downstream cancellation",
    ));
    let manager = manager(Arc::clone(&state), Duration::from_secs(60), false);
    let (session_id, statement_id) = open_session(&manager, "\0anonymous");
    let (endpoint, server) = start_server(manager, Duration::from_secs(1), false).await;
    let error = tokio::task::spawn_blocking(move || {
        client(&endpoint, None)
            .call_unary(
                protocol::method::EXECUTE,
                &statement_request(&session_id, &statement_id),
                None,
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    let wire = wire_error(error);
    assert_eq!(wire.status, "cancelled");
    assert_eq!(wire.vendor_code, 8675);
    assert_eq!(wire.sqlstate, vec![72, 89, 48, 48, 56]);
    assert!(!wire.details.is_empty());
    server.abort();
}
