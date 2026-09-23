//! ADBC 1.1 client driver for the ADBC proxy service.

mod iroh_pool;

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{
    CancelHandle, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};
use adbc_proxy_protocol as protocol;
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use rustls::pki_types::pem::PemObject;
use vgi_rpc_client::{HttpClient, Metadata, RpcClient, RpcError};
use vgi_rpc_iroh::IrohTarget;

pub const DRIVER_NAME: &str = "adbc_driver_proxy";
pub const OPTION_PROXY_URI: &str = "adbc.proxy.uri";
pub const OPTION_TARGET: &str = "adbc.proxy.target";
pub const OPTION_BEARER_TOKEN: &str = "adbc.proxy.auth.bearer_token";
pub const OPTION_REQUEST_TIMEOUT_MS: &str = "adbc.proxy.request_timeout_ms";
pub const OPTION_MAX_RESPONSE_BYTES: &str = "adbc.proxy.max_response_bytes";
pub const OPTION_MAX_BIND_BYTES: &str = "adbc.proxy.max_bind_bytes";
pub const OPTION_TLS_CA: &str = "adbc.proxy.tls.ca";
pub const OPTION_TLS_CERT: &str = "adbc.proxy.tls.cert";
pub const OPTION_TLS_KEY: &str = "adbc.proxy.tls.key";
pub const OPTION_TLS_SERVER_NAME: &str = "adbc.proxy.tls.server_name";
pub const OPTION_IROH_SECRET_KEY: &str = "adbc.proxy.iroh.secret_key";
pub const OPTION_IROH_DIRECT_ADDRESS: &str = "adbc.proxy.iroh.direct_address";
const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES: i64 = 256 * 1024 * 1024;
const DEFAULT_MAX_BIND_BYTES: i64 = protocol::MAX_BIND_STREAM_BYTES as i64;

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
        if key == OPTION_MAX_BIND_BYTES && value > protocol::MAX_CONFIGURABLE_BIND_BYTES {
            return Err(invalid(format!(
                "option {key:?} must not exceed {}",
                protocol::MAX_CONFIGURABLE_BIND_BYTES
            )));
        }
        Ok(value)
    }

    fn remote_options(&self) -> Vec<protocol::WireOption> {
        // `uri` historically doubled as the proxy endpoint.  Once the explicit
        // proxy URI is present, preserve the standard ADBC `uri` option for the
        // downstream driver.
        let legacy_uri_is_proxy = !self.options.contains_key(OPTION_PROXY_URI);
        self.options
            .iter()
            .filter(|(key, _)| {
                !is_proxy_database_option(key) && !(legacy_uri_is_proxy && key.as_str() == "uri")
            })
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
        let max_bind_bytes =
            self.proxy_positive_int(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)?;
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
            max_bind_bytes,
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
        let schema = batch.schema();
        let mut batches = std::iter::once(Ok(batch));
        self.remote.bind_batches(
            protocol::method::BIND,
            &self.statement_id,
            schema,
            &mut batches,
        )
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let schema = reader.schema();
        let mut reader = reader;
        self.remote.bind_batches(
            protocol::method::BIND_STREAM,
            &self.statement_id,
            schema,
            &mut reader,
        )
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

#[derive(Clone, Default)]
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
    max_bind_bytes: usize,
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
    _iroh_lease: Option<iroh_pool::Lease>,
    connector: ByteConnector,
}

#[derive(Clone)]
struct ByteConnector {
    endpoint: String,
    request_timeout: Duration,
    options: TransportOptions,
}

impl ByteConnector {
    fn connect(&self) -> Result<(RpcClient, Option<iroh_pool::Lease>)> {
        let client = if self.endpoint.starts_with("tcp://") {
            let (host, port) = host_and_port(&self.endpoint, "tcp")?;
            RpcClient::tcp_connect_with_timeout(&host, port, Some(self.request_timeout))
                .map_err(rpc_error)?
        } else if self.endpoint.starts_with("tls+tcp://") {
            let (host, port) = host_and_port(&self.endpoint, "tls+tcp")?;
            tls_tcp_client(&host, port, self.request_timeout, &self.options)?
        } else if self.endpoint.starts_with("iroh://") {
            return self.connect_iroh();
        } else {
            return Err(not_implemented("unsupported byte-stream proxy URI"));
        };
        Ok((configure_rpc_client(client), None))
    }

    fn connect_iroh(&self) -> Result<(RpcClient, Option<iroh_pool::Lease>)> {
        let remote_id = IrohTarget::parse(&self.endpoint)
            .map_err(|error| invalid(error.to_string()))?
            .endpoint_id();
        let secret_key = self
            .options
            .iroh_secret_key
            .as_ref()
            .map(|secret| {
                iroh::SecretKey::from_str(secret.trim())
                    .map_err(|error| invalid(format!("invalid Iroh client secret key: {error}")))
            })
            .transpose()?;
        let direct_address = self
            .options
            .iroh_direct_address
            .as_ref()
            .map(|address| {
                address
                    .parse()
                    .map_err(|error| invalid(format!("invalid Iroh direct address: {error}")))
            })
            .transpose()?;
        let pooled = iroh_pool::open_client(iroh_pool::Config {
            remote_id,
            direct_address,
            secret_key,
            rpc_timeout: self.request_timeout,
        })
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
        let (client, lease) = pooled.into_parts();
        Ok((configure_rpc_client(client), Some(lease)))
    }
}

enum RemoteTransport {
    Http(HttpTransport),
    Byte(Box<ByteTransport>),
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

        if !endpoint.starts_with("tcp://")
            && !endpoint.starts_with("tls+tcp://")
            && !endpoint.starts_with("iroh://")
        {
            return Err(not_implemented(
                "proxy URI scheme; supported schemes are http, https, tcp, tls+tcp, and iroh",
            ));
        }
        let connector = ByteConnector {
            endpoint,
            request_timeout,
            options,
        };
        let (client, lease) = connector.connect()?;
        Ok(Self::Byte(Box::new(ByteTransport {
            client: Mutex::new(client),
            _iroh_lease: lease,
            connector,
        })))
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
    max_bind_bytes: usize,
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
            max_bind_bytes,
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
            max_bind_bytes,
        })
    }

    fn client(&self) -> Result<HttpClient> {
        self.transport.http_client()
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        self.transport.call(method, request)
    }

    fn bind_batches(
        &self,
        method: &str,
        statement_id: &str,
        schema: SchemaRef,
        batches: &mut dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
    ) -> Result<()> {
        let schema_ipc =
            protocol::encode_schema(schema.as_ref()).map_err(|error| invalid(error.to_string()))?;
        let init = RecordBatch::try_new(
            protocol::bind_init_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.session_id.clone()])),
                Arc::new(StringArray::from(vec![statement_id.to_string()])),
                Arc::new(BinaryArray::from_vec(vec![schema_ipc.as_slice()])),
            ],
        )?;
        let mut logical_bytes = 0usize;
        let send = |exchange: &mut dyn FnMut(&RecordBatch, Option<&Metadata>) -> Result<()>| {
            for batch in batches {
                let batch = batch?;
                logical_bytes = logical_bytes
                    .checked_add(batch.get_array_memory_size())
                    .ok_or_else(|| invalid("bind stream size overflow"))?;
                if logical_bytes > self.max_bind_bytes {
                    return Err(invalid(format!(
                        "bind stream exceeds the {} byte limit (at least {logical_bytes} bytes)",
                        self.max_bind_bytes
                    )));
                }
                exchange(&batch, None)?;
            }
            let finish = RecordBatch::new_empty(schema);
            let metadata = Metadata::from([(
                protocol::BIND_FINISH_METADATA_KEY.to_string(),
                "1".to_string(),
            )]);
            exchange(&finish, Some(&metadata))
        };

        match &self.transport {
            RemoteTransport::Http(_) => {
                let mut client = self.client()?;
                let mut stream = client
                    .open_exchange(method, &init, None, false)
                    .map_err(rpc_error)?;
                let result = send(&mut |batch, metadata| {
                    stream
                        .exchange(batch, metadata)
                        .map_err(rpc_error)?
                        .ok_or_else(|| internal("bind exchange ended before acknowledgement"))?;
                    Ok(())
                });
                if result.is_err() {
                    let _ = stream.cancel();
                }
                result
            }
            RemoteTransport::Byte(byte) => {
                let mut client = byte
                    .client
                    .lock()
                    .map_err(|_| internal("VGI byte-stream client is poisoned"))?;
                let mut stream = client
                    .open_exchange(method, &init, None, false)
                    .map_err(rpc_error)?;
                let result = send(&mut |batch, metadata| {
                    stream
                        .exchange(batch, metadata)
                        .map_err(rpc_error)?
                        .ok_or_else(|| internal("bind exchange ended before acknowledgement"))?;
                    Ok(())
                });
                if result.is_err() {
                    let _ = stream.cancel();
                }
                result
            }
        }
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
    Byte(ByteReader),
}

enum ByteReaderCommand {
    Next(mpsc::Sender<Result<Option<RecordBatch>>>),
    Cancel,
}

struct ByteReader {
    tx: SyncSender<ByteReaderCommand>,
}

impl ByteReader {
    fn open(connector: ByteConnector, request: RecordBatch) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::Builder::new()
            .name("adbc-proxy-result-stream".to_string())
            .spawn(move || {
                let (mut client, lease) = match connector.connect() {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                let mut stream = match client.open_producer(
                    protocol::method::READ_RESULT,
                    &request,
                    None,
                    false,
                ) {
                    Ok(stream) => stream,
                    Err(error) => {
                        let _ = ready_tx.send(Err(rpc_error(error)));
                        return;
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                while let Ok(command) = rx.recv() {
                    match command {
                        ByteReaderCommand::Next(reply) => {
                            let value = stream
                                .tick()
                                .map(|value| value.map(|(batch, _)| batch))
                                .map_err(rpc_error);
                            let finished = matches!(value, Ok(None)) || value.is_err();
                            let _ = reply.send(value);
                            if finished {
                                break;
                            }
                        }
                        ByteReaderCommand::Cancel => {
                            let _ = stream.cancel();
                            break;
                        }
                    }
                }
                drop(stream);
                drop(lease);
            })
            .map_err(|error| internal(format!("start result stream worker: {error}")))?;
        ready_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped during startup"))??;
        Ok(Self { tx })
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(ByteReaderCommand::Next(reply_tx))
            .map_err(|_| internal("result stream worker stopped"))?;
        reply_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped"))?
    }
}

impl Drop for ByteReader {
    fn drop(&mut self) {
        let _ = self.tx.send(ByteReaderCommand::Cancel);
    }
}

impl RemoteReader {
    fn open(remote: Arc<RemoteConnection>, result_id: String, schema: SchemaRef) -> Result<Self> {
        if !remote.transport.is_http() {
            let request = RecordBatch::try_new(
                protocol::read_result_schema(),
                vec![
                    Arc::new(StringArray::from(vec![remote.session_id.clone()])),
                    Arc::new(StringArray::from(vec![result_id.clone()])),
                    Arc::new(Int64Array::from(vec![0])),
                ],
            )?;
            let RemoteTransport::Byte(byte) = &remote.transport else {
                unreachable!();
            };
            let reader = ByteReader::open(byte.connector.clone(), request)?;
            return Ok(Self {
                remote,
                result_id,
                schema,
                mode: RemoteReaderMode::Byte(reader),
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
            RemoteReaderMode::Byte(reader) => {
                let batch = reader.next()?;
                self.finished = batch.is_none();
                Ok(batch)
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
    RpcClient::tls_tcp_connect(
        host,
        port,
        server_name,
        Arc::new(config),
        timeout,
        Some(timeout),
    )
    .map_err(rpc_error)
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
            | OPTION_MAX_BIND_BYTES
            | OPTION_TLS_CA
            | OPTION_TLS_CERT
            | OPTION_TLS_KEY
            | OPTION_TLS_SERVER_NAME
            | OPTION_IROH_SECRET_KEY
            | OPTION_IROH_DIRECT_ADDRESS
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
        assert!(is_proxy_database_option(OPTION_MAX_BIND_BYTES));
        assert!(!is_proxy_database_option("username"));
    }

    #[test]
    fn explicit_proxy_uri_preserves_downstream_uri() {
        let mut database = ProxyDatabase::default();
        database
            .set_option(
                OptionDatabase::Other(OPTION_PROXY_URI.into()),
                "http://localhost:8080".into(),
            )
            .unwrap();
        database
            .set_option(
                OptionDatabase::Uri,
                "postgresql://database.example/app".into(),
            )
            .unwrap();

        let options = database.remote_options();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key, "uri");
        assert_eq!(
            options[0].value,
            protocol::WireOptionValue::String("postgresql://database.example/app".into())
        );
    }

    #[test]
    fn legacy_uri_endpoint_is_not_forwarded() {
        let mut database = ProxyDatabase::default();
        database
            .set_option(OptionDatabase::Uri, "http://localhost:8080".into())
            .unwrap();

        assert!(database.remote_options().is_empty());
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

    #[test]
    fn max_bind_bytes_is_positive_and_supports_the_full_adbc_integer_range() {
        let mut database = ProxyDatabase::default();
        assert_eq!(
            database
                .proxy_positive_int(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .unwrap(),
            protocol::MAX_BIND_STREAM_BYTES
        );
        database.options.insert(
            OPTION_MAX_BIND_BYTES.into(),
            OptionValue::Int(protocol::MAX_CONFIGURABLE_BIND_BYTES as i64),
        );
        assert_eq!(
            database
                .proxy_positive_int(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .unwrap(),
            protocol::MAX_CONFIGURABLE_BIND_BYTES
        );
        database
            .options
            .insert(OPTION_MAX_BIND_BYTES.into(), OptionValue::Int(0));
        assert!(
            database
                .proxy_positive_int(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .is_err()
        );
    }
}
