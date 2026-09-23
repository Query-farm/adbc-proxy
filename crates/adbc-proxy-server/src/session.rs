use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::CancelHandle;
use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use uuid::Uuid;

use crate::backend::{Backend, BackendConnection, BackendStatement};
use crate::config::TargetConfig;

#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    pub max_sessions: usize,
    pub max_sessions_per_principal: usize,
    pub max_statements_per_session: usize,
    pub max_results_per_session: usize,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            max_sessions_per_principal: 32,
            max_statements_per_session: 64,
            max_results_per_session: 64,
        }
    }
}

impl SessionLimits {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_sessions == 0
            || self.max_sessions_per_principal == 0
            || self.max_statements_per_session == 0
            || self.max_results_per_session == 0
        {
            return Err("all session, statement, and result limits must be positive");
        }
        if self.max_sessions_per_principal > self.max_sessions {
            return Err("max_sessions_per_principal must not exceed max_sessions");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct TargetAuthorizer {
    permissions: Option<HashMap<String, HashSet<String>>>,
}

impl TargetAuthorizer {
    pub fn new(permissions: HashMap<String, Vec<String>>) -> Self {
        if permissions.is_empty() {
            return Self::default();
        }
        Self {
            permissions: Some(
                permissions
                    .into_iter()
                    .map(|(principal, targets)| (principal, targets.into_iter().collect()))
                    .collect(),
            ),
        }
    }

    fn allows(&self, principal: &str, target: &str) -> bool {
        let Some(permissions) = &self.permissions else {
            return true;
        };
        let principal_name = principal
            .split_once('\0')
            .map_or(principal, |(_, name)| name);
        permissions
            .get(principal_name)
            .or_else(|| permissions.get(principal))
            .is_some_and(|targets| targets.contains("*") || targets.contains(target))
    }
}

pub struct SessionManager {
    backend: Arc<dyn Backend>,
    targets: HashMap<String, TargetConfig>,
    registry: Mutex<SessionRegistry>,
    ttl: Duration,
    require_authentication: bool,
    limits: SessionLimits,
    authorizer: TargetAuthorizer,
}

#[derive(Default)]
struct SessionRegistry {
    sessions: HashMap<String, Arc<Session>>,
    opening_total: usize,
    opening_by_principal: HashMap<String, usize>,
}

pub struct Session {
    principal: String,
    target: String,
    last_used: Mutex<Instant>,
    connection: Mutex<Box<dyn BackendConnection>>,
    connection_cancel: Arc<dyn CancelHandle>,
    statements: Mutex<HashMap<String, Arc<StatementEntry>>>,
    results: Mutex<HashMap<String, Arc<Mutex<ResultEntry>>>>,
    limits: SessionLimits,
}

pub struct StatementEntry {
    pub statement: Mutex<Box<dyn BackendStatement>>,
    pub cancel: Arc<dyn CancelHandle>,
}

pub struct ResultEntry {
    owner_statement_id: Option<String>,
    reader: Box<dyn RecordBatchReader + Send + 'static>,
    schema: SchemaRef,
    next_sequence: i64,
    last: Option<(i64, RecordBatch)>,
    finished: bool,
}

impl SessionManager {
    /// Construct a manager with production-safe default quotas and no target
    /// restrictions. `with_limits_and_authorizer` is used by the service
    /// binary to apply its validated configuration.
    pub fn new(
        backend: Arc<dyn Backend>,
        targets: HashMap<String, TargetConfig>,
        ttl: Duration,
        require_authentication: bool,
    ) -> Self {
        Self::with_limits_and_authorizer(
            backend,
            targets,
            ttl,
            require_authentication,
            SessionLimits::default(),
            TargetAuthorizer::default(),
        )
    }

    pub fn with_limits_and_authorizer(
        backend: Arc<dyn Backend>,
        targets: HashMap<String, TargetConfig>,
        ttl: Duration,
        require_authentication: bool,
        limits: SessionLimits,
        authorizer: TargetAuthorizer,
    ) -> Self {
        debug_assert!(limits.validate().is_ok());
        Self {
            backend,
            targets,
            registry: Mutex::new(SessionRegistry::default()),
            ttl,
            require_authentication,
            limits,
            authorizer,
        }
    }

    pub fn principal(&self, auth: &vgi_rpc::AuthContext) -> vgi_rpc::Result<String> {
        if self.require_authentication {
            auth.require_authenticated()?;
        }
        if auth.authenticated {
            Ok(format!("{}\0{}", auth.domain, auth.principal))
        } else {
            Ok("\0anonymous".to_string())
        }
    }

    pub fn open(
        &self,
        principal: String,
        target_name: &str,
        database_options: Vec<(String, adbc_core::options::OptionValue)>,
        connection_options: Vec<(String, adbc_core::options::OptionValue)>,
    ) -> Result<String, AdbcError> {
        if !self.authorizer.allows(&principal, target_name) {
            return Err(AdbcError::with_message_and_status(
                "principal is not authorized for the requested target",
                Status::Unauthorized,
            ));
        }
        let target = self.targets.get(target_name).ok_or_else(|| {
            AdbcError::with_message_and_status("target is not configured", Status::NotFound)
        })?;

        self.reserve_open(&principal)?;
        let connection = self
            .backend
            .open(target, database_options, connection_options);

        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        registry.opening_total = registry.opening_total.saturating_sub(1);
        if let Some(opening) = registry.opening_by_principal.get_mut(&principal) {
            *opening = opening.saturating_sub(1);
            if *opening == 0 {
                registry.opening_by_principal.remove(&principal);
            }
        }
        let connection = connection?;
        let connection_cancel = connection.cancel_handle();
        let id = Uuid::new_v4().to_string();
        registry.sessions.insert(
            id.clone(),
            Arc::new(Session {
                principal,
                target: target_name.to_string(),
                last_used: Mutex::new(Instant::now()),
                connection: Mutex::new(connection),
                connection_cancel,
                statements: Mutex::new(HashMap::new()),
                results: Mutex::new(HashMap::new()),
                limits: self.limits,
            }),
        );
        Ok(id)
    }

    fn reserve_open(&self, principal: &str) -> Result<(), AdbcError> {
        // Reaping before admission ensures dead leases do not consume quota
        // until the next background interval.
        self.reap_expired()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let total = registry.sessions.len() + registry.opening_total;
        if total >= self.limits.max_sessions {
            return Err(quota("global session"));
        }
        let active_for_principal = registry
            .sessions
            .values()
            .filter(|session| session.principal == principal)
            .count();
        let opening_for_principal = registry
            .opening_by_principal
            .get(principal)
            .copied()
            .unwrap_or_default();
        if active_for_principal + opening_for_principal >= self.limits.max_sessions_per_principal {
            return Err(quota("per-principal session"));
        }
        registry.opening_total += 1;
        *registry
            .opening_by_principal
            .entry(principal.to_string())
            .or_default() += 1;
        Ok(())
    }

    pub fn get(&self, id: &str, principal: &str) -> Result<Arc<Session>, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let session = registry
            .sessions
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("session"))?;
        if session.principal != principal {
            return Err(AdbcError::with_message_and_status(
                "session does not belong to the authenticated principal",
                Status::Unauthorized,
            ));
        }
        let mut last_used = session
            .last_used
            .lock()
            .map_err(|_| internal("session lease is poisoned"))?;
        if last_used.elapsed() > self.ttl {
            let removed = registry.sessions.remove(id);
            drop(last_used);
            drop(registry);
            drop(removed);
            return Err(not_found("expired session"));
        }
        *last_used = Instant::now();
        drop(last_used);
        Ok(session)
    }

    pub fn close(&self, id: &str, principal: &str) -> Result<(), AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let session = registry
            .sessions
            .get(id)
            .ok_or_else(|| not_found("session"))?;
        if session.principal != principal {
            return Err(AdbcError::with_message_and_status(
                "session does not belong to the authenticated principal",
                Status::Unauthorized,
            ));
        }
        let removed = registry.sessions.remove(id);
        drop(registry);
        drop(removed);
        Ok(())
    }

    /// Remove expired leases. In-flight calls retain their `Arc<Session>` and
    /// are allowed to finish; no subsequent call can reacquire the session.
    pub fn reap_expired(&self) -> Result<usize, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let mut expired = Vec::new();
        for (id, session) in &registry.sessions {
            let last_used = session
                .last_used
                .lock()
                .map_err(|_| internal("session lease is poisoned"))?;
            if last_used.elapsed() > self.ttl {
                expired.push(id.clone());
            }
        }
        let removed: Vec<_> = expired
            .iter()
            .filter_map(|id| registry.sessions.remove(id))
            .collect();
        drop(registry);
        drop(removed);
        Ok(expired.len())
    }

    /// Detach every session during graceful shutdown. Active calls keep their
    /// resources until they return; all idle resources are dropped now.
    pub fn close_all(&self) -> Result<usize, AdbcError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| internal("session registry is poisoned"))?;
        let sessions = std::mem::take(&mut registry.sessions);
        let count = sessions.len();
        drop(registry);
        drop(sessions);
        Ok(count)
    }

    #[cfg(test)]
    fn session_count(&self) -> usize {
        self.registry
            .lock()
            .expect("session registry")
            .sessions
            .len()
    }
}

impl Session {
    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut dyn BackendConnection) -> Result<T, AdbcError>,
    ) -> Result<T, AdbcError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| internal("connection is poisoned"))?;
        operation(connection.as_mut())
    }

    pub fn cancel_connection(&self) -> Result<(), AdbcError> {
        self.connection_cancel.try_cancel()
    }

    pub fn commit(&self) -> Result<(), AdbcError> {
        self.connection
            .lock()
            .map_err(|_| internal("connection is poisoned"))?
            .commit()
    }

    pub fn rollback(&self) -> Result<(), AdbcError> {
        self.connection
            .lock()
            .map_err(|_| internal("connection is poisoned"))?
            .rollback()
    }

    pub fn new_statement(&self) -> Result<String, AdbcError> {
        let mut statements = self
            .statements
            .lock()
            .map_err(|_| internal("statement registry is poisoned"))?;
        if statements.len() >= self.limits.max_statements_per_session {
            return Err(quota("statement"));
        }
        let statement = self
            .connection
            .lock()
            .map_err(|_| internal("connection is poisoned"))?
            .new_statement()?;
        let cancel = statement.cancel_handle();
        let id = Uuid::new_v4().to_string();
        statements.insert(
            id.clone(),
            Arc::new(StatementEntry {
                statement: Mutex::new(statement),
                cancel,
            }),
        );
        Ok(id)
    }

    pub fn statement(&self, id: &str) -> Result<Arc<StatementEntry>, AdbcError> {
        self.statements
            .lock()
            .map_err(|_| internal("statement registry is poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("statement"))
    }

    pub fn close_statement(&self, id: &str) -> Result<(), AdbcError> {
        self.statements
            .lock()
            .map_err(|_| internal("statement registry is poisoned"))?
            .remove(id)
            .ok_or_else(|| not_found("statement"))?;
        self.invalidate_statement_results(id)?;
        Ok(())
    }

    pub fn insert_result(
        &self,
        reader: Box<dyn RecordBatchReader + Send + 'static>,
    ) -> Result<(String, SchemaRef), AdbcError> {
        self.insert_owned_result(None, reader)
    }

    pub fn insert_statement_result(
        &self,
        statement_id: &str,
        reader: Box<dyn RecordBatchReader + Send + 'static>,
    ) -> Result<(String, SchemaRef), AdbcError> {
        self.invalidate_statement_results(statement_id)?;
        self.insert_owned_result(Some(statement_id.to_string()), reader)
    }

    pub fn invalidate_statement_results(&self, statement_id: &str) -> Result<(), AdbcError> {
        self.results
            .lock()
            .map_err(|_| internal("result registry is poisoned"))?
            .retain(|_, result| {
                result
                    .lock()
                    .map(|entry| entry.owner_statement_id.as_deref() != Some(statement_id))
                    .unwrap_or(false)
            });
        Ok(())
    }

    fn insert_owned_result(
        &self,
        owner_statement_id: Option<String>,
        reader: Box<dyn RecordBatchReader + Send + 'static>,
    ) -> Result<(String, SchemaRef), AdbcError> {
        let schema = reader.schema();
        let id = Uuid::new_v4().to_string();
        let mut results = self
            .results
            .lock()
            .map_err(|_| internal("result registry is poisoned"))?;
        if results.len() >= self.limits.max_results_per_session {
            return Err(quota("result"));
        }
        results.insert(
            id.clone(),
            Arc::new(Mutex::new(ResultEntry {
                owner_statement_id,
                reader,
                schema: schema.clone(),
                next_sequence: 0,
                last: None,
                finished: false,
            })),
        );
        Ok((id, schema))
    }

    pub fn result(&self, id: &str) -> Result<Arc<Mutex<ResultEntry>>, AdbcError> {
        self.results
            .lock()
            .map_err(|_| internal("result registry is poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("result"))
    }

    pub fn close_result(&self, id: &str) -> Result<(), AdbcError> {
        self.results
            .lock()
            .map_err(|_| internal("result registry is poisoned"))?
            .remove(id)
            .ok_or_else(|| not_found("result"))?;
        Ok(())
    }
}

impl ResultEntry {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn next(&mut self, sequence: i64) -> Result<Option<RecordBatch>, AdbcError> {
        if let Some((last_sequence, batch)) = &self.last
            && sequence == *last_sequence
        {
            return Ok(Some(batch.clone()));
        }
        if sequence != self.next_sequence {
            return Err(AdbcError::with_message_and_status(
                format!(
                    "invalid result sequence: expected {}, got {sequence}",
                    self.next_sequence
                ),
                Status::InvalidState,
            ));
        }
        if self.finished {
            return Ok(None);
        }
        match self.reader.next() {
            Some(Ok(batch)) => {
                self.last = Some((sequence, batch.clone()));
                self.next_sequence += 1;
                Ok(Some(batch))
            }
            Some(Err(error)) => Err(error.into()),
            None => {
                self.finished = true;
                self.last = None;
                Ok(None)
            }
        }
    }
}

fn quota(kind: &str) -> AdbcError {
    AdbcError::with_message_and_status(format!("{kind} quota exceeded"), Status::InvalidState)
}

fn internal(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::Internal)
}

fn not_found(kind: &str) -> AdbcError {
    AdbcError::with_message_and_status(format!("{kind} was not found"), Status::NotFound)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use adbc_core::error::{Error, Result, Status};
    use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
    use adbc_core::{CancelHandle, PartitionedResult};
    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{ArrowError, Schema};

    use super::{SessionLimits, SessionManager, TargetAuthorizer};
    use crate::backend::{Backend, BackendConnection, BackendStatement};
    use crate::config::TargetConfig;

    struct DummyBackend;
    struct DummyConnection;
    struct DummyStatement;
    struct DummyCancel;

    impl Backend for DummyBackend {
        fn open(
            &self,
            _target: &TargetConfig,
            _database_options: Vec<(String, OptionValue)>,
            _connection_options: Vec<(String, OptionValue)>,
        ) -> Result<Box<dyn BackendConnection>> {
            Ok(Box::new(DummyConnection))
        }
    }

    impl BackendConnection for DummyConnection {
        fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
            Arc::new(DummyCancel)
        }

        fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
            Ok(Box::new(DummyStatement))
        }

        fn set_option(&mut self, _key: &str, _value: OptionValue) -> Result<()> {
            unsupported()
        }

        fn get_option_string(&self, _key: &str) -> Result<String> {
            unsupported()
        }

        fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            unsupported()
        }

        fn get_option_int(&self, _key: &str) -> Result<i64> {
            unsupported()
        }

        fn get_option_double(&self, _key: &str) -> Result<f64> {
            unsupported()
        }

        fn get_info(
            &self,
            _codes: Option<HashSet<InfoCode>>,
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
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
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_table_schema(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: &str,
        ) -> Result<Schema> {
            unsupported()
        }

        fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn get_statistics(
            &self,
            _catalog: Option<&str>,
            _db_schema: Option<&str>,
            _table_name: Option<&str>,
            _approximate: bool,
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        fn rollback(&mut self) -> Result<()> {
            Ok(())
        }

        fn read_partition(
            &self,
            _partition: &[u8],
        ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }
    }

    impl BackendStatement for DummyStatement {
        fn set_option(&mut self, _key: &str, _value: OptionValue) -> Result<()> {
            unsupported()
        }

        fn get_option_string(&self, _key: &str) -> Result<String> {
            unsupported()
        }

        fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            unsupported()
        }

        fn get_option_int(&self, _key: &str) -> Result<i64> {
            unsupported()
        }

        fn get_option_double(&self, _key: &str) -> Result<f64> {
            unsupported()
        }

        fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
            unsupported()
        }

        fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
            unsupported()
        }

        fn set_sql_query(&mut self, _query: &str) -> Result<()> {
            unsupported()
        }

        fn set_substrait_plan(&mut self, _plan: &[u8]) -> Result<()> {
            unsupported()
        }

        fn prepare(&mut self) -> Result<()> {
            unsupported()
        }

        fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
            unsupported()
        }

        fn execute_update(&mut self) -> Result<Option<i64>> {
            unsupported()
        }

        fn execute_schema(&mut self) -> Result<Schema> {
            unsupported()
        }

        fn execute_partitions(&mut self) -> Result<PartitionedResult> {
            unsupported()
        }

        fn get_parameter_schema(&self) -> Result<Schema> {
            unsupported()
        }

        fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
            Arc::new(DummyCancel)
        }
    }

    impl CancelHandle for DummyCancel {
        fn try_cancel(&self) -> Result<()> {
            Ok(())
        }
    }

    fn unsupported<T>() -> Result<T> {
        Err(Error::with_message_and_status(
            "not used by session tests",
            Status::NotImplemented,
        ))
    }

    fn target() -> TargetConfig {
        TargetConfig {
            driver: "dummy".to_string(),
            entrypoint: None,
            database_options: Vec::new(),
            connection_options: Vec::new(),
            allow_client_database_options: false,
            allow_client_connection_options: false,
        }
    }

    fn manager(
        limits: SessionLimits,
        authorizer: TargetAuthorizer,
        ttl: Duration,
    ) -> SessionManager {
        SessionManager::with_limits_and_authorizer(
            Arc::new(DummyBackend),
            HashMap::from([("sqlite".to_string(), target())]),
            ttl,
            true,
            limits,
            authorizer,
        )
    }

    fn open(manager: &SessionManager, principal: &str) -> String {
        manager
            .open(
                format!("bearer\0{principal}"),
                "sqlite",
                Vec::new(),
                Vec::new(),
            )
            .expect("open session")
    }

    fn empty_reader() -> Box<dyn RecordBatchReader + Send + 'static> {
        let schema = Arc::new(Schema::empty());
        Box::new(RecordBatchIterator::new(
            Vec::<std::result::Result<RecordBatch, ArrowError>>::new(),
            schema,
        ))
    }

    #[test]
    fn enforces_target_authorization_and_principal_isolation() {
        let authorizer = TargetAuthorizer::new(HashMap::from([(
            "alice".to_string(),
            vec!["sqlite".to_string()],
        )]));
        let manager = manager(
            SessionLimits::default(),
            authorizer,
            Duration::from_secs(60),
        );

        let id = open(&manager, "alice");
        let error = match manager.get(&id, "bearer\0bob") {
            Ok(_) => panic!("another principal must not acquire the session"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::Unauthorized);
        let error = manager
            .open("bearer\0bob".to_string(), "sqlite", Vec::new(), Vec::new())
            .expect_err("unlisted principal must be denied");
        assert_eq!(error.status, Status::Unauthorized);
    }

    #[test]
    fn enforces_session_statement_and_result_quotas() {
        let limits = SessionLimits {
            max_sessions: 2,
            max_sessions_per_principal: 1,
            max_statements_per_session: 1,
            max_results_per_session: 1,
        };
        let manager = manager(limits, TargetAuthorizer::default(), Duration::from_secs(60));
        let alice = open(&manager, "alice");
        assert!(
            manager
                .open(
                    "bearer\0alice".to_string(),
                    "sqlite",
                    Vec::new(),
                    Vec::new()
                )
                .is_err()
        );
        let _bob = open(&manager, "bob");
        assert!(
            manager
                .open(
                    "bearer\0charlie".to_string(),
                    "sqlite",
                    Vec::new(),
                    Vec::new()
                )
                .is_err()
        );

        let session = manager
            .get(&alice, "bearer\0alice")
            .expect("get own session");
        let statement = session.new_statement().expect("first statement");
        assert!(session.new_statement().is_err());
        session
            .close_statement(&statement)
            .expect("release statement quota");
        session
            .new_statement()
            .expect("statement quota was released");

        let (result, _) = session.insert_result(empty_reader()).expect("first result");
        assert!(session.insert_result(empty_reader()).is_err());
        session.close_result(&result).expect("release result quota");
        session
            .insert_result(empty_reader())
            .expect("result quota was released");
    }

    #[test]
    fn reaps_expired_sessions_and_closes_everything() {
        let manager = manager(
            SessionLimits::default(),
            TargetAuthorizer::default(),
            Duration::from_millis(1),
        );
        let id = open(&manager, "alice");
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(manager.reap_expired().expect("reap"), 1);
        assert_eq!(manager.session_count(), 0);
        let error = match manager.get(&id, "bearer\0alice") {
            Ok(_) => panic!("expired session must not be returned"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::NotFound);

        open(&manager, "alice");
        open(&manager, "bob");
        assert_eq!(manager.close_all().expect("close all"), 2);
        assert_eq!(manager.session_count(), 0);
    }

    #[test]
    fn validates_limit_relationships() {
        let invalid = SessionLimits {
            max_sessions: 1,
            max_sessions_per_principal: 2,
            max_statements_per_session: 1,
            max_results_per_session: 1,
        };
        assert!(invalid.validate().is_err());
    }
}
