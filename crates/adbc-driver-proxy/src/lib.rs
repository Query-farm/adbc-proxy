//! ADBC 1.1 client driver for the ADBC proxy service.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{
    CancelHandle, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};
use adbc_proxy_protocol as protocol;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use rustls::pki_types::pem::PemObject;
use vgi_rpc_client::{HttpClient, RpcClient, RpcError, Transport};
use vgi_rpc_iroh::{IrohClientOptions, IrohConnection};

pub const DRIVER_NAME: &str = "adbc_driver_proxy";
pub const OPTION_PROXY_URI: &str = "adbc.proxy.uri";
pub const OPTION_TARGET: &str = "adbc.proxy.target";
pub const OPTION_BEARER_TOKEN: &str = "adbc.proxy.auth.bearer_token";
pub const OPTION_REQUEST_TIMEOUT_MS: &str = "adbc.proxy.request_timeout_ms";
pub const OPTION_MAX_RESPONSE_BYTES: &str = "adbc.proxy.max_response_bytes";
pub const OPTION_TLS_CA: &str = "adbc.proxy.tls.ca";
pub const OPTION_TLS_CERT: &str = "adbc.proxy.tls.cert";
pub const OPTION_TLS_KEY: &str = "adbc.proxy.tls.key";
pub const OPTION_TLS_SERVER_NAME: &str = "adbc.proxy.tls.server_name";
pub const OPTION_IROH_SECRET_KEY: &str = "adbc.proxy.iroh.secret_key";
pub const OPTION_IROH_DIRECT_ADDRESS: &str = "adbc.proxy.iroh.direct_address";
const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES: i64 = 256 * 1024 * 1024;

#[derive(Default)]
pub struct ProxyDriver;

impl Driver for ProxyDriver {
    type DatabaseType = ProxyDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(ProxyDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = ProxyDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        database.validate()?;
        Ok(database)
    }
}

#[derive(Default)]
pub struct ProxyDatabase {
    options: HashMap<String, OptionValue>,
}

impl ProxyDatabase {
    fn validate(&self) -> Result<()> {
        self.string_option_any(&[OPTION_PROXY_URI, OptionDatabase::Uri.as_ref()])?;
        self.string_option(OPTION_TARGET)?;
        Ok(())
    }

    fn string_option(&self, key: &str) -> Result<String> {
        match self.options.get(key) {
            Some(OptionValue::String(value)) => Ok(value.clone()),
            Some(_) => Err(invalid(format!("option {key:?} must be a string"))),
            None => Err(invalid(format!("required option {key:?} is missing"))),
        }
    }

    fn string_option_any(&self, keys: &[&str]) -> Result<String> {
        for key in keys {
            if self.options.contains_key(*key) {
                return self.string_option(key);
            }
        }
        Err(invalid(format!("required option {:?} is missing", keys[0])))
    }

    fn optional_string(&self, key: &str) -> Result<Option<String>> {
        self.options
            .get(key)
            .map(|value| match value {
                OptionValue::String(value) => Ok(value.clone()),
                _ => Err(invalid(format!("option {key:?} must be a string"))),
            })
            .transpose()
    }

    fn proxy_positive_int(&self, key: &str, default: i64) -> Result<usize> {
        let value = match self.options.get(key) {
            None => default,
            Some(OptionValue::Int(value)) => *value,
            Some(_) => return Err(invalid(format!("option {key:?} must be an integer"))),
        };
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| invalid(format!("option {key:?} must be positive")))?;
        if key == OPTION_MAX_RESPONSE_BYTES && value < 65_536 {
            return Err(invalid(format!("option {key:?} must be at least 65536")));
        }
        Ok(value)
    }

    fn remote_options(&self) -> Vec<protocol::WireOption> {
        self.options
            .iter()
            .filter(|(key, _)| !is_proxy_database_option(key))
            .map(|(key, value)| protocol::WireOption {
                key: key.clone(),
                value: protocol::WireOptionValue::from(value),
            })
            .collect()
    }
}

impl Optionable for ProxyDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.options.insert(key.as_ref().to_string(), value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_string(&self.options, key.as_ref())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_bytes(&self.options, key.as_ref())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_int(&self.options, key.as_ref())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_double(&self.options, key.as_ref())
    }
}

impl Database for ProxyDatabase {
    type ConnectionType = ProxyConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(std::iter::empty())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        self.validate()?;
        let endpoint = self.string_option_any(&[OPTION_PROXY_URI, OptionDatabase::Uri.as_ref()])?;
        let target = self.string_option(OPTION_TARGET)?;
        let bearer_token = self
            .options
            .get(OPTION_BEARER_TOKEN)
            .map(|_| self.string_option(OPTION_BEARER_TOKEN))
            .transpose()?;
        let request_timeout_ms =
            self.proxy_positive_int(OPTION_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS)?;
        let max_response_bytes =
            self.proxy_positive_int(OPTION_MAX_RESPONSE_BYTES, DEFAULT_MAX_RESPONSE_BYTES)?;
        let transport_options = TransportOptions {
            tls_ca: self.optional_string(OPTION_TLS_CA)?,
            tls_cert: self.optional_string(OPTION_TLS_CERT)?,
            tls_key: self.optional_string(OPTION_TLS_KEY)?,
            tls_server_name: self.optional_string(OPTION_TLS_SERVER_NAME)?,
            iroh_secret_key: self.optional_string(OPTION_IROH_SECRET_KEY)?,
            iroh_direct_address: self.optional_string(OPTION_IROH_DIRECT_ADDRESS)?,
        };
        let connection_options = opts
            .into_iter()
            .map(|(key, value)| protocol::WireOption {
                key: key.as_ref().to_string(),
                value: protocol::WireOptionValue::from(&value),
            })
            .collect::<Vec<_>>();
        let state = RemoteConnection::open(RemoteConnectionOptions {
            endpoint,
            bearer_token,
            target,
            database_options: self.remote_options(),
            connection_options,
            request_timeout_ms,
            max_response_bytes,
            transport_options,
        })?;
        Ok(ProxyConnection {
            remote: Arc::new(state),
        })
    }
}

pub struct ProxyConnection {
    remote: Arc<RemoteConnection>,
}

impl Optionable for ProxyConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let request = connection_option_request(&self.remote.session_id, key.as_ref(), &value)?;
        self.remote
            .call(protocol::method::SET_CONNECTION_OPTION, &request)?;
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.remote.get_connection_option(key.as_ref(), "string")? {
            OptionValue::String(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match self.remote.get_connection_option(key.as_ref(), "bytes")? {
            OptionValue::Bytes(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.remote.get_connection_option(key.as_ref(), "int")? {
            OptionValue::Int(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.remote.get_connection_option(key.as_ref(), "double")? {
            OptionValue::Double(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }
}

impl Connection for ProxyConnection {
    type StatementType = ProxyStatement;

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(ProxyConnectionCancelHandle {
            remote: Arc::downgrade(&self.remote),
        })
    }

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let request = session_request(&self.remote.session_id)?;
        let response = self
            .remote
            .call(protocol::method::NEW_STATEMENT, &request)?;
        let statement_id = string_column(&response, "statement_id")?.to_string();
        Ok(ProxyStatement {
            remote: self.remote.clone(),
            statement_id,
        })
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let codes = codes.map(|values| values.iter().map(u32::from).collect::<Vec<_>>());
        self.remote.connection_stream_call(
            protocol::method::GET_INFO,
            serde_json::json!({ "codes": codes }),
        )
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote.connection_stream_call(
            protocol::method::GET_OBJECTS,
            serde_json::json!({
                "depth": i32::from(depth),
                "catalog": catalog,
                "db_schema": db_schema,
                "table_name": table_name,
                "table_type": table_type,
                "column_name": column_name,
            }),
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.remote.connection_schema_call(
            protocol::method::GET_TABLE_SCHEMA,
            serde_json::json!({
                "catalog": catalog,
                "db_schema": db_schema,
                "table_name": table_name,
            }),
        )
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote
            .session_stream_call(protocol::method::GET_TABLE_TYPES)
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote
            .session_stream_call(protocol::method::GET_STATISTIC_NAMES)
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote.connection_stream_call(
            protocol::method::GET_STATISTICS,
            serde_json::json!({
                "catalog": catalog,
                "db_schema": db_schema,
                "table_name": table_name,
                "approximate": approximate,
            }),
        )
    }

    fn commit(&mut self) -> Result<()> {
        self.remote.session_call(protocol::method::COMMIT)
    }

    fn rollback(&mut self) -> Result<()> {
        self.remote.session_call(protocol::method::ROLLBACK)
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let request = connection_binary_request(&self.remote.session_id, partition.as_ref())?;
        let response = self
            .remote
            .call(protocol::method::READ_PARTITION, &request)?;
        self.remote.reader_from_response(&response)
    }
}

struct ProxyConnectionCancelHandle {
    remote: std::sync::Weak<RemoteConnection>,
}

impl CancelHandle for ProxyConnectionCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        let Some(remote) = self.remote.upgrade() else {
            return Ok(());
        };
        remote.session_call(protocol::method::CANCEL_CONNECTION)
    }
}

pub struct ProxyStatement {
    remote: Arc<RemoteConnection>,
    statement_id: String,
}

impl ProxyStatement {
    fn request(&self) -> Result<RecordBatch> {
        statement_request(&self.remote.session_id, &self.statement_id)
    }

    fn get_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let request = statement_option_key_request(
            &self.remote.session_id,
            &self.statement_id,
            key,
            value_type,
        )?;
        let response = self
            .remote
            .call(protocol::method::GET_STATEMENT_OPTION, &request)?;
        decode_option_response(&response)
    }
}

impl Drop for ProxyStatement {
    fn drop(&mut self) {
        if let Ok(request) = self.request() {
            let _ = self
                .remote
                .call(protocol::method::CLOSE_STATEMENT, &request);
        }
    }
}

impl Optionable for ProxyStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let request = statement_option_request(
            &self.remote.session_id,
            &self.statement_id,
            key.as_ref(),
            &value,
        )?;
        self.remote
            .call(protocol::method::SET_STATEMENT_OPTION, &request)?;
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.get_option(key.as_ref(), "string")? {
            OptionValue::String(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match self.get_option(key.as_ref(), "bytes")? {
            OptionValue::Bytes(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.get_option(key.as_ref(), "int")? {
            OptionValue::Int(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.get_option(key.as_ref(), "double")? {
            OptionValue::Double(value) => Ok(value),
            _ => Err(internal("proxy returned the wrong option value type")),
        }
    }
}

impl Statement for ProxyStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let bytes = protocol::encode_batches(batch.schema().as_ref(), [Ok(batch)])
            .map_err(|error| invalid(error.to_string()))?;
        let request =
            statement_binary_request(&self.remote.session_id, &self.statement_id, &bytes)?;
        self.remote.call(protocol::method::BIND, &request)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let schema = reader.schema();
        let bytes = protocol::encode_batches(schema.as_ref(), reader)
            .map_err(|error| invalid(error.to_string()))?;
        let request =
            statement_binary_request(&self.remote.session_id, &self.statement_id, &bytes)?;
        self.remote.call(protocol::method::BIND_STREAM, &request)?;
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE, &self.request()?)?;
        let result_id = string_column(&response, "result_id")?.to_string();
        let schema = protocol::decode_schema(binary_column(&response, "schema_ipc")?)
            .map_err(|error| internal(error.to_string()))?;
        let reader = RemoteReader::open(self.remote.clone(), result_id, Arc::new(schema))?;
        Ok(Box::new(reader))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_UPDATE, &self.request()?)?;
        optional_i64_column(&response, "rows_affected")
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_SCHEMA, &self.request()?)?;
        decode_schema_response(&response)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_PARTITIONS, &self.request()?)?;
        let partitions = serde_json::from_str(string_column(&response, "partitions_json")?)
            .map_err(|error| internal(format!("invalid partitions response: {error}")))?;
        Ok(PartitionedResult {
            partitions,
            schema: decode_schema_response(&response)?,
            rows_affected: required_i64_column(&response, "rows_affected")?,
        })
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        let response = self
            .remote
            .call(protocol::method::GET_PARAMETER_SCHEMA, &self.request()?)?;
        decode_schema_response(&response)
    }

    fn prepare(&mut self) -> Result<()> {
        self.remote
            .call(protocol::method::PREPARE, &self.request()?)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        let request = RecordBatch::try_new(
            protocol::set_sql_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.remote.session_id.clone()])),
                Arc::new(StringArray::from(vec![self.statement_id.clone()])),
                Arc::new(StringArray::from(vec![query.as_ref().to_string()])),
            ],
        )?;
        self.remote
            .call(protocol::method::SET_SQL_QUERY, &request)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        let request =
            statement_binary_request(&self.remote.session_id, &self.statement_id, plan.as_ref())?;
        self.remote
            .call(protocol::method::SET_SUBSTRAIT_PLAN, &request)?;
        Ok(())
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(ProxyCancelHandle {
            remote: Arc::downgrade(&self.remote),
            statement_id: self.statement_id.clone(),
        })
    }
}

struct ProxyCancelHandle {
    remote: std::sync::Weak<RemoteConnection>,
    statement_id: String,
}

impl CancelHandle for ProxyCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        let Some(remote) = self.remote.upgrade() else {
            return Ok(());
        };
        let request = statement_request(&remote.session_id, &self.statement_id)?;
        remote.call(protocol::method::CANCEL_STATEMENT, &request)?;
        Ok(())
    }
}

#[derive(Default)]
struct TransportOptions {
    tls_ca: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_server_name: Option<String>,
    iroh_secret_key: Option<String>,
    iroh_direct_address: Option<String>,
}

struct RemoteConnectionOptions {
    endpoint: String,
    bearer_token: Option<String>,
    target: String,
    database_options: Vec<protocol::WireOption>,
    connection_options: Vec<protocol::WireOption>,
    request_timeout_ms: usize,
    max_response_bytes: usize,
    transport_options: TransportOptions,
}

struct HttpTransport {
    endpoint: String,
    bearer_token: Option<String>,
    http: reqwest::blocking::Client,
    request_timeout: Duration,
    max_response_bytes: usize,
}

struct ByteTransport {
    client: Mutex<RpcClient>,
    _runtime: Option<tokio::runtime::Runtime>,
    _iroh_endpoint: Option<iroh::Endpoint>,
    _iroh_connection: Option<IrohConnection>,
}

enum RemoteTransport {
    Http(HttpTransport),
    Byte(ByteTransport),
}

impl RemoteTransport {
    fn connect(
        endpoint: String,
        bearer_token: Option<String>,
        request_timeout: Duration,
        max_response_bytes: usize,
        options: TransportOptions,
    ) -> Result<Self> {
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            let http = reqwest::blocking::Client::builder()
                .build()
                .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
            return Ok(Self::Http(HttpTransport {
                endpoint,
                bearer_token,
                http,
                request_timeout,
                max_response_bytes,
            }));
        }
        if bearer_token.is_some() {
            return Err(invalid(
                "bearer-token authentication is available only for HTTP; use mTLS identity for tls+tcp:// or endpoint identity for iroh://",
            ));
        }

        let client = if endpoint.starts_with("tcp://") {
            let (host, port) = host_and_port(&endpoint, "tcp")?;
            RpcClient::tcp_connect_with_timeout(&host, port, Some(request_timeout))
                .map_err(rpc_error)?
        } else if endpoint.starts_with("tls+tcp://") {
            let (host, port) = host_and_port(&endpoint, "tls+tcp")?;
            tls_tcp_client(&host, port, request_timeout, &options)?
        } else if endpoint.starts_with("iroh://") {
            return Self::connect_iroh(endpoint, request_timeout, options);
        } else {
            return Err(not_implemented(
                "proxy URI scheme; supported schemes are http, https, tcp, tls+tcp, and iroh",
            ));
        };
        Ok(Self::Byte(ByteTransport {
            client: Mutex::new(configure_rpc_client(client)),
            _runtime: None,
            _iroh_endpoint: None,
            _iroh_connection: None,
        }))
    }

    fn connect_iroh(
        target: String,
        request_timeout: Duration,
        options: TransportOptions,
    ) -> Result<Self> {
        let parsed = url::Url::parse(&target).map_err(|_| invalid("invalid iroh:// URI"))?;
        if parsed.scheme() != "iroh"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid(
                "Iroh URI must be iroh://<endpoint-id> without credentials, port, path, query, or fragment",
            ));
        }
        let remote_id = iroh::EndpointId::from_str(
            parsed
                .host_str()
                .ok_or_else(|| invalid("Iroh endpoint ID is required"))?,
        )
        .map_err(|error| invalid(format!("invalid Iroh endpoint ID: {error}")))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| internal(format!("create Iroh runtime: {error}")))?;
        let (iroh_endpoint, connection, client) = runtime.block_on(async move {
            let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0);
            if let Some(secret) = options.iroh_secret_key {
                let secret = iroh::SecretKey::from_str(secret.trim())
                    .map_err(|error| invalid(format!("invalid Iroh client secret key: {error}")))?;
                builder = builder.secret_key(secret);
            }
            let local = builder
                .bind()
                .await
                .map_err(|error| internal(format!("bind Iroh client endpoint: {error}")))?;
            let mut remote = iroh::EndpointAddr::new(remote_id);
            if let Some(address) = options.iroh_direct_address {
                let address = address
                    .parse()
                    .map_err(|error| invalid(format!("invalid Iroh direct address: {error}")))?;
                remote = remote.with_ip_addr(address);
            }
            let connection = IrohConnection::connect_addr(
                local.clone(),
                remote,
                IrohClientOptions::default().with_rpc_timeout(request_timeout),
            )
            .await
            .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
            let client = connection
                .open_client()
                .await
                .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
            Ok::<_, Error>((local, connection, client))
        })?;
        Ok(Self::Byte(ByteTransport {
            client: Mutex::new(configure_rpc_client(client)),
            _runtime: Some(runtime),
            _iroh_endpoint: Some(iroh_endpoint),
            _iroh_connection: Some(connection),
        }))
    }

    fn is_http(&self) -> bool {
        matches!(self, Self::Http(_))
    }

    fn http_client(&self) -> Result<HttpClient> {
        let Self::Http(http) = self else {
            return Err(internal(
                "HTTP stream requested for a byte-stream transport",
            ));
        };
        build_client(
            &http.endpoint,
            http.bearer_token.as_deref(),
            &http.http,
            http.request_timeout,
            http.max_response_bytes,
        )
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        match self {
            Self::Http(_) => {
                let mut client = self.http_client()?;
                client
                    .call_unary(method, request, None)
                    .map(|(batch, _)| batch)
                    .map_err(rpc_error)
            }
            Self::Byte(byte) => byte
                .client
                .lock()
                .map_err(|_| internal("VGI byte-stream client is poisoned"))?
                .call_unary(method, request, None)
                .map(|(batch, _)| batch)
                .map_err(rpc_error),
        }
    }
}

struct RemoteConnection {
    transport: RemoteTransport,
    session_id: String,
}

impl RemoteConnection {
    fn open(options: RemoteConnectionOptions) -> Result<Self> {
        let RemoteConnectionOptions {
            endpoint,
            bearer_token,
            target,
            database_options,
            connection_options,
            request_timeout_ms,
            max_response_bytes,
            transport_options,
        } = options;
        let request_timeout = Duration::from_millis(request_timeout_ms as u64);
        let transport = RemoteTransport::connect(
            endpoint,
            bearer_token,
            request_timeout,
            max_response_bytes,
            transport_options,
        )?;
        let request = RecordBatch::try_new(
            protocol::open_connection_schema(),
            vec![
                Arc::new(StringArray::from(vec![target])),
                Arc::new(StringArray::from(vec![
                    protocol::encode_options(&database_options)
                        .map_err(|error| invalid(error.to_string()))?,
                ])),
                Arc::new(StringArray::from(vec![
                    protocol::encode_options(&connection_options)
                        .map_err(|error| invalid(error.to_string()))?,
                ])),
            ],
        )?;
        let response = transport.call(protocol::method::OPEN_CONNECTION, &request)?;
        let session_id = string_column(&response, "session_id")?.to_string();
        Ok(Self {
            transport,
            session_id,
        })
    }

    fn client(&self) -> Result<HttpClient> {
        self.transport.http_client()
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        self.transport.call(method, request)
    }

    fn session_call(&self, method: &str) -> Result<()> {
        self.call(method, &session_request(&self.session_id)?)?;
        Ok(())
    }

    fn get_connection_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let request = connection_option_key_request(&self.session_id, key, value_type)?;
        let response = self.call(protocol::method::GET_CONNECTION_OPTION, &request)?;
        decode_option_response(&response)
    }

    fn reader_from_response(
        self: &Arc<Self>,
        response: &RecordBatch,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let result_id = string_column(response, "result_id")?.to_string();
        let schema = protocol::decode_schema(binary_column(response, "schema_ipc")?)
            .map_err(|error| internal(error.to_string()))?;
        Ok(Box::new(RemoteReader::open(
            self.clone(),
            result_id,
            Arc::new(schema),
        )?))
    }

    fn connection_stream_call(
        self: &Arc<Self>,
        method: &str,
        args: serde_json::Value,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let request = connection_args_request(&self.session_id, args)?;
        let response = self.call(method, &request)?;
        self.reader_from_response(&response)
    }

    fn session_stream_call(
        self: &Arc<Self>,
        method: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self.call(method, &session_request(&self.session_id)?)?;
        self.reader_from_response(&response)
    }

    fn connection_schema_call(&self, method: &str, args: serde_json::Value) -> Result<Schema> {
        let request = connection_args_request(&self.session_id, args)?;
        let response = self.call(method, &request)?;
        decode_schema_response(&response)
    }
}

impl Drop for RemoteConnection {
    fn drop(&mut self) {
        if let Ok(request) = session_request(&self.session_id) {
            let _ = self.call(protocol::method::CLOSE_CONNECTION, &request);
        }
    }
}

struct RemoteReader {
    remote: Arc<RemoteConnection>,
    result_id: String,
    schema: SchemaRef,
    mode: RemoteReaderMode,
    finished: bool,
}

enum RemoteReaderMode {
    Http {
        pending: VecDeque<RecordBatch>,
        continuation: Option<String>,
    },
    Byte {
        sequence: i64,
    },
}

impl RemoteReader {
    fn open(remote: Arc<RemoteConnection>, result_id: String, schema: SchemaRef) -> Result<Self> {
        if !remote.transport.is_http() {
            return Ok(Self {
                remote,
                result_id,
                schema,
                mode: RemoteReaderMode::Byte { sequence: 0 },
                finished: false,
            });
        }
        let request = RecordBatch::try_new(
            protocol::read_result_schema(),
            vec![
                Arc::new(StringArray::from(vec![remote.session_id.clone()])),
                Arc::new(StringArray::from(vec![result_id.clone()])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )?;
        let (first, continuation, finished) = {
            let mut client = remote.client()?;
            let mut stream = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .map_err(rpc_error)?;
            let first = stream.next_with_token().map_err(rpc_error)?;
            let finished = stream.is_finished();
            match first {
                Some(((batch, _), continuation)) => (Some(batch), continuation, finished),
                None => (None, None, true),
            }
        };
        Ok(Self {
            remote,
            result_id,
            schema,
            mode: RemoteReaderMode::Http {
                pending: first.into_iter().collect(),
                continuation,
            },
            finished,
        })
    }

    fn next_remote(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        match &mut self.mode {
            RemoteReaderMode::Http {
                pending,
                continuation,
            } => {
                if let Some(batch) = pending.pop_front() {
                    return Ok(Some(batch));
                }
                let Some(token) = continuation.take() else {
                    self.finished = true;
                    return Ok(None);
                };
                let (batch, next_continuation, finished) = {
                    let mut client = self.remote.client()?;
                    let mut stream = client.resume_stream(protocol::method::READ_RESULT, token);
                    let value = stream.next_with_token().map_err(rpc_error)?;
                    let finished = stream.is_finished();
                    match value {
                        Some(((batch, _), continuation)) => (Some(batch), continuation, finished),
                        None => (None, None, true),
                    }
                };
                *continuation = next_continuation;
                self.finished = finished || continuation.is_none();
                Ok(batch)
            }
            RemoteReaderMode::Byte { sequence } => {
                let request = RecordBatch::try_new(
                    protocol::read_result_schema(),
                    vec![
                        Arc::new(StringArray::from(vec![self.remote.session_id.clone()])),
                        Arc::new(StringArray::from(vec![self.result_id.clone()])),
                        Arc::new(Int64Array::from(vec![*sequence])),
                    ],
                )?;
                let response = self
                    .remote
                    .call(protocol::method::READ_RESULT_BATCH, &request)?;
                let finished = boolean_column(&response, "finished")?;
                if finished {
                    self.finished = true;
                    return Ok(None);
                }
                let payload = binary_column(&response, "payload")?.to_vec();
                let batch = protocol::decode_result_batch(payload)
                    .map_err(|error| internal(error.to_string()))?;
                *sequence += 1;
                Ok(Some(batch))
            }
        }
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        if let Ok(request) = result_request(&self.remote.session_id, &self.result_id) {
            let _ = self.remote.call(protocol::method::CLOSE_RESULT, &request);
        }
    }
}

impl Iterator for RemoteReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_remote() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(error) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for RemoteReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn session_request(session_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::session_schema(),
        vec![Arc::new(StringArray::from(vec![session_id.to_string()]))],
    )?)
}

fn statement_request(session_id: &str, statement_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
        ],
    )?)
}

fn connection_args_request(session_id: &str, args: serde_json::Value) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::connection_args_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![args.to_string()])),
        ],
    )?)
}

fn connection_binary_request(session_id: &str, payload: &[u8]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::connection_binary_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(BinaryArray::from_vec(vec![payload])),
        ],
    )?)
}

fn connection_option_request(
    session_id: &str,
    key: &str,
    value: &OptionValue,
) -> Result<RecordBatch> {
    let value = serde_json::to_string(&protocol::WireOptionValue::from(value))
        .map_err(|error| invalid(error.to_string()))?;
    Ok(RecordBatch::try_new(
        protocol::connection_option_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )?)
}

fn connection_option_key_request(
    session_id: &str,
    key: &str,
    value_type: &str,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::connection_option_key_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value_type.to_string()])),
        ],
    )?)
}

fn statement_option_request(
    session_id: &str,
    statement_id: &str,
    key: &str,
    value: &OptionValue,
) -> Result<RecordBatch> {
    let value = serde_json::to_string(&protocol::WireOptionValue::from(value))
        .map_err(|error| invalid(error.to_string()))?;
    Ok(RecordBatch::try_new(
        protocol::statement_option_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )?)
}

fn statement_option_key_request(
    session_id: &str,
    statement_id: &str,
    key: &str,
    value_type: &str,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_option_key_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value_type.to_string()])),
        ],
    )?)
}

fn statement_binary_request(
    session_id: &str,
    statement_id: &str,
    payload: &[u8],
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_binary_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
            Arc::new(BinaryArray::from_vec(vec![payload])),
        ],
    )?)
}

fn result_request(session_id: &str, result_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::result_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![result_id.to_string()])),
        ],
    )?)
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a str> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| internal(format!("response is missing column {name:?}")))?;
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal(format!("response column {name:?} is not UTF-8")))?;
    if batch.num_rows() != 1 || values.is_null(0) {
        return Err(internal(format!(
            "response column {name:?} must contain one non-null value"
        )));
    }
    Ok(values.value(0))
}

fn binary_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a [u8]> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| internal(format!("response is missing column {name:?}")))?;
    let values = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| internal(format!("response column {name:?} is not binary")))?;
    if batch.num_rows() != 1 || values.is_null(0) {
        return Err(internal(format!(
            "response column {name:?} must contain one non-null value"
        )));
    }
    Ok(values.value(0))
}

fn boolean_column(batch: &RecordBatch, name: &str) -> Result<bool> {
    let (index, _) = batch
        .schema()
        .column_with_name(name)
        .ok_or_else(|| internal(format!("proxy response is missing {name:?}")))?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| internal(format!("proxy response column {name:?} is not boolean")))?;
    if batch.num_rows() != 1 || values.is_null(0) {
        return Err(internal(format!(
            "proxy response column {name:?} must contain one non-null value"
        )));
    }
    Ok(values.value(0))
}

fn optional_i64_column(batch: &RecordBatch, name: &str) -> Result<Option<i64>> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| internal(format!("response is missing column {name:?}")))?;
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal(format!("response column {name:?} is not int64")))?;
    if batch.num_rows() != 1 {
        return Err(internal("response must contain exactly one row"));
    }
    Ok((!values.is_null(0)).then(|| values.value(0)))
}

fn required_i64_column(batch: &RecordBatch, name: &str) -> Result<i64> {
    optional_i64_column(batch, name)?
        .ok_or_else(|| internal(format!("response column {name:?} must not be null")))
}

fn decode_schema_response(batch: &RecordBatch) -> Result<Schema> {
    protocol::decode_schema(binary_column(batch, "schema_ipc")?)
        .map_err(|error| internal(error.to_string()))
}

fn decode_option_response(batch: &RecordBatch) -> Result<OptionValue> {
    let value =
        serde_json::from_str::<protocol::WireOptionValue>(string_column(batch, "value_json")?)
            .map_err(|error| internal(format!("invalid option response: {error}")))?;
    value
        .into_adbc()
        .map_err(|error| internal(error.to_string()))
}

fn build_client(
    endpoint: &str,
    bearer_token: Option<&str>,
    http: &reqwest::blocking::Client,
    request_timeout: Duration,
    max_response_bytes: usize,
) -> Result<HttpClient> {
    let mut builder = HttpClient::connect(endpoint.to_string())
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .timeout(Some(request_timeout))
        .accepted_max_response_bytes(max_response_bytes)
        .client(http.clone());
    if let Some(token) = bearer_token {
        let value = format!("Bearer {token}");
        builder = builder.header("authorization", &value).map_err(rpc_error)?;
    }
    builder.build().map_err(rpc_error)
}

fn configure_rpc_client(client: RpcClient) -> RpcClient {
    client
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
}

fn host_and_port(endpoint: &str, expected_scheme: &str) -> Result<(String, u16)> {
    let parsed = url::Url::parse(endpoint).map_err(|_| invalid("invalid proxy URI"))?;
    if parsed.scheme() != expected_scheme
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(format!(
            "{expected_scheme} proxy URI must contain only a host and explicit port"
        )));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| invalid("proxy URI host is required"))?
        .to_string();
    let port = parsed
        .port()
        .ok_or_else(|| invalid("proxy URI port is required"))?;
    Ok((host, port))
}

type ClientTlsStream = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

struct LocalTlsTransport {
    reader: TlsReader,
    writer: TlsWriter,
    reusable: Arc<AtomicBool>,
}

struct TlsReader {
    stream: Arc<Mutex<ClientTlsStream>>,
    reusable: Arc<AtomicBool>,
}

struct TlsWriter {
    stream: Arc<Mutex<ClientTlsStream>>,
    reusable: Arc<AtomicBool>,
}

impl Read for TlsReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let result = self
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read(buffer);
        if result.is_err() {
            self.reusable.store(false, Ordering::Release);
        }
        result
    }
}

impl Write for TlsWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let result = self
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write(buffer);
        if result.is_err() {
            self.reusable.store(false, Ordering::Release);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .flush();
        if result.is_err() {
            self.reusable.store(false, Ordering::Release);
        }
        result
    }
}

impl Transport for LocalTlsTransport {
    fn split(&mut self) -> (&mut dyn Read, &mut dyn Write) {
        (&mut self.reader, &mut self.writer)
    }

    fn is_reusable(&self) -> bool {
        self.reusable.load(Ordering::Acquire)
    }

    fn close(&mut self) -> std::result::Result<(), RpcError> {
        self.reusable.store(false, Ordering::Release);
        let mut stream = self
            .writer
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stream.conn.send_close_notify();
        let _ = stream.flush();
        stream
            .sock
            .shutdown(std::net::Shutdown::Both)
            .map_err(|error| RpcError::new("TransportError", format!("close TLS socket: {error}")))
    }
}

fn tls_tcp_client(
    host: &str,
    port: u16,
    timeout: Duration,
    options: &TransportOptions,
) -> Result<RpcClient> {
    let ca_path = options
        .tls_ca
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_CA} is required for tls+tcp://")))?;
    let cert_path = options
        .tls_cert
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_CERT} is required for tls+tcp://")))?;
    let key_path = options
        .tls_key
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_KEY} is required for tls+tcp://")))?;
    let server_name = options.tls_server_name.as_deref().unwrap_or(host);

    let mut roots = rustls::RootCertStore::empty();
    for certificate in read_certificates(ca_path)? {
        roots
            .add(certificate)
            .map_err(|error| invalid(format!("invalid TLS CA certificate: {error}")))?;
    }
    let certificates = read_certificates(cert_path)?;
    let private_key = read_private_key(key_path)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| internal(format!("configure TLS versions: {error}")))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, private_key)
        .map_err(|error| invalid(format!("invalid TLS client certificate or key: {error}")))?;
    let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
        .map_err(|_| invalid("invalid TLS server name"))?;
    let socket = std::net::TcpStream::connect((host, port))
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
    socket
        .set_nodelay(true)
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
    socket
        .set_read_timeout(Some(timeout))
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
    socket
        .set_write_timeout(Some(timeout))
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
    let connection = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|error| internal(format!("create TLS connection: {error}")))?;
    let mut stream = rustls::StreamOwned::new(connection, socket);
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| invalid("TLS handshake timeout exceeds Instant"))?;
    while stream.conn.is_handshaking() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::with_message_and_status(
                "TLS handshake timed out",
                Status::IO,
            ));
        }
        stream
            .sock
            .set_read_timeout(Some(remaining))
            .and_then(|()| stream.sock.set_write_timeout(Some(remaining)))
            .map_err(|error| {
                Error::with_message_and_status(
                    format!("configure TLS handshake timeout: {error}"),
                    Status::IO,
                )
            })?;
        stream.conn.complete_io(&mut stream.sock).map_err(|error| {
            Error::with_message_and_status(format!("TLS handshake failed: {error}"), Status::IO)
        })?;
    }
    stream
        .sock
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.sock.set_write_timeout(Some(timeout)))
        .map_err(|error| {
            Error::with_message_and_status(
                format!("configure TLS I/O timeout: {error}"),
                Status::IO,
            )
        })?;
    let stream = Arc::new(Mutex::new(stream));
    let reusable = Arc::new(AtomicBool::new(true));
    Ok(RpcClient::from_transport(Box::new(LocalTlsTransport {
        reader: TlsReader {
            stream: Arc::clone(&stream),
            reusable: Arc::clone(&reusable),
        },
        writer: TlsWriter {
            stream,
            reusable: Arc::clone(&reusable),
        },
        reusable,
    })))
}

fn read_certificates(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certificates = rustls::pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|error| {
            invalid(format!(
                "could not open TLS certificate file {path:?}: {error}"
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("could not parse TLS certificate file: {error}")))?;
    if certificates.is_empty() {
        return Err(invalid(format!(
            "TLS certificate file {path:?} contains no certificates"
        )));
    }
    Ok(certificates)
}

fn read_private_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    rustls::pki_types::PrivateKeyDer::from_pem_file(path)
        .map_err(|error| invalid(format!("could not load TLS private key {path:?}: {error}")))
}

fn rpc_error(error: RpcError) -> Error {
    if error.error_type == "AdbcError"
        && let Ok(wire) = serde_json::from_str::<protocol::WireAdbcError>(&error.message)
    {
        return wire.into_adbc();
    }
    Error::with_message_and_status(error.to_string(), Status::IO)
}

fn is_proxy_database_option(key: &str) -> bool {
    matches!(
        key,
        OPTION_PROXY_URI
            | OPTION_TARGET
            | OPTION_BEARER_TOKEN
            | OPTION_REQUEST_TIMEOUT_MS
            | OPTION_MAX_RESPONSE_BYTES
            | OPTION_TLS_CA
            | OPTION_TLS_CERT
            | OPTION_TLS_KEY
            | OPTION_TLS_SERVER_NAME
            | OPTION_IROH_SECRET_KEY
            | OPTION_IROH_DIRECT_ADDRESS
            | "uri"
    )
}

fn get_string(options: &HashMap<String, OptionValue>, key: &str) -> Result<String> {
    match options.get(key) {
        Some(OptionValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(invalid(format!("option {key:?} is not a string"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_bytes(options: &HashMap<String, OptionValue>, key: &str) -> Result<Vec<u8>> {
    match options.get(key) {
        Some(OptionValue::Bytes(value)) => Ok(value.clone()),
        Some(_) => Err(invalid(format!("option {key:?} is not bytes"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_int(options: &HashMap<String, OptionValue>, key: &str) -> Result<i64> {
    match options.get(key) {
        Some(OptionValue::Int(value)) => Ok(*value),
        Some(_) => Err(invalid(format!("option {key:?} is not an integer"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_double(options: &HashMap<String, OptionValue>, key: &str) -> Result<f64> {
    match options.get(key) {
        Some(OptionValue::Double(value)) => Ok(*value),
        Some(_) => Err(invalid(format!("option {key:?} is not a double"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

fn internal(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::Internal)
}

fn not_found(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::NotFound)
}

fn not_implemented(feature: &str) -> Error {
    Error::with_message_and_status(
        format!("{feature} is not implemented by adbc_driver_proxy yet"),
        Status::NotImplemented,
    )
}

adbc_ffi::export_driver!(AdbcDriverProxyInit, ProxyDriver);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_options_are_not_forwarded() {
        assert!(is_proxy_database_option(OPTION_PROXY_URI));
        assert!(is_proxy_database_option(OPTION_TARGET));
        assert!(!is_proxy_database_option("username"));
    }

    #[test]
    fn database_requires_endpoint_and_target() {
        let mut database = ProxyDatabase::default();
        assert_eq!(
            database.validate().unwrap_err().status,
            Status::InvalidArguments
        );
        database
            .set_option(OptionDatabase::Uri, "http://localhost:8080".into())
            .unwrap();
        database
            .set_option(OptionDatabase::Other(OPTION_TARGET.into()), "sqlite".into())
            .unwrap();
        database.validate().unwrap();
    }
}
