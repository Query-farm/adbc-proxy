//! Versioned Arrow schemas and wire values for ADBC-over-VGI.

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use adbc_core::error::{Error as AdbcError, Status};
use adbc_core::options::OptionValue;
use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::{IpcDataGenerator, IpcWriteOptions, StreamWriter};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PROTOCOL_NAME: &str = "org.queryfarm.AdbcProxy.v1";
pub const PROTOCOL_VERSION: &str = "0.1.0";
/// Maximum encoded parameter stream accepted by the initial protocol binding.
/// This makes the buffering behavior of `bind_stream` explicit and bounded.
pub const MAX_BIND_STREAM_BYTES: usize = 64 * 1024 * 1024;
/// Current deployed VGI Rust implementation's message compatibility ceiling.
/// This is an implementation guard, not Arrow's fundamental body-length limit.
pub const MAX_VGI_MESSAGE_BYTES: usize = u32::MAX as usize;
/// Reserved space for the VGI Arrow envelope around a nested bind IPC stream.
pub const BIND_ENVELOPE_HEADROOM_BYTES: usize = 1024 * 1024;
/// Largest configurable monolithic bind stream in this protocol version.
///
/// `statement_binary_schema` carries the nested IPC stream in Arrow `Binary`,
/// whose offsets are signed 32-bit. Supporting larger binds requires a
/// protocol change to chunk/externalize them (preferred) or use `LargeBinary`.
pub const MAX_CONFIGURABLE_BIND_BYTES: usize = i32::MAX as usize - BIND_ENVELOPE_HEADROOM_BYTES;
const _: () = assert!(MAX_CONFIGURABLE_BIND_BYTES < MAX_VGI_MESSAGE_BYTES);

pub mod method {
    pub const OPEN_CONNECTION: &str = "open_connection";
    pub const CLOSE_CONNECTION: &str = "close_connection";
    pub const COMMIT: &str = "commit";
    pub const ROLLBACK: &str = "rollback";
    pub const CANCEL_CONNECTION: &str = "cancel_connection";
    pub const SET_CONNECTION_OPTION: &str = "set_connection_option";
    pub const GET_CONNECTION_OPTION: &str = "get_connection_option";
    pub const GET_INFO: &str = "get_info";
    pub const GET_OBJECTS: &str = "get_objects";
    pub const GET_TABLE_SCHEMA: &str = "get_table_schema";
    pub const GET_TABLE_TYPES: &str = "get_table_types";
    pub const GET_STATISTIC_NAMES: &str = "get_statistic_names";
    pub const GET_STATISTICS: &str = "get_statistics";
    pub const READ_PARTITION: &str = "read_partition";
    pub const NEW_STATEMENT: &str = "new_statement";
    pub const CLOSE_STATEMENT: &str = "close_statement";
    pub const SET_SQL_QUERY: &str = "set_sql_query";
    pub const PREPARE: &str = "prepare";
    pub const CANCEL_STATEMENT: &str = "cancel_statement";
    pub const SET_STATEMENT_OPTION: &str = "set_statement_option";
    pub const GET_STATEMENT_OPTION: &str = "get_statement_option";
    pub const BIND: &str = "bind";
    pub const BIND_STREAM: &str = "bind_stream";
    pub const EXECUTE: &str = "execute";
    pub const EXECUTE_UPDATE: &str = "execute_update";
    pub const EXECUTE_SCHEMA: &str = "execute_schema";
    pub const EXECUTE_PARTITIONS: &str = "execute_partitions";
    pub const GET_PARAMETER_SCHEMA: &str = "get_parameter_schema";
    pub const SET_SUBSTRAIT_PLAN: &str = "set_substrait_plan";
    pub const CLOSE_RESULT: &str = "close_result";
    pub const READ_RESULT: &str = "read_result";
    /// Byte-stream-friendly one-batch pull. Unlike `read_result`, this is a
    /// unary operation and therefore keeps one persistent TCP/Iroh transport
    /// synchronized between batches without HTTP continuation tokens.
    pub const READ_RESULT_BATCH: &str = "read_result_batch";
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("missing column {0:?}")]
    MissingColumn(String),
    #[error("column {name:?} has the wrong type; expected {expected}")]
    WrongType {
        name: String,
        expected: &'static str,
    },
    #[error("request must contain exactly one row, got {0}")]
    RowCount(usize),
    #[error("null is not allowed in column {0:?}")]
    Null(String),
    #[error("invalid options JSON: {0}")]
    OptionsJson(#[from] serde_json::Error),
    #[error("invalid Arrow IPC schema: {0}")]
    Arrow(#[from] ArrowError),
    #[error("invalid Arrow IPC schema message")]
    InvalidSchemaMessage,
    #[error("Arrow IPC stream does not contain a record batch")]
    EmptyStream,
    #[error("Arrow IPC stream exceeds the {limit} byte limit ({actual} bytes)")]
    StreamTooLarge { limit: usize, actual: usize },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum WireOptionValue {
    String(String),
    Bytes(String),
    Int(i64),
    Double(f64),
}

impl WireOptionValue {
    pub fn into_adbc(self) -> Result<OptionValue, ProtocolError> {
        Ok(match self {
            Self::String(value) => OptionValue::String(value),
            Self::Bytes(value) => OptionValue::Bytes(
                base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(|error| {
                        ProtocolError::OptionsJson(serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            error,
                        )))
                    })?,
            ),
            Self::Int(value) => OptionValue::Int(value),
            Self::Double(value) => OptionValue::Double(value),
        })
    }
}

impl From<&OptionValue> for WireOptionValue {
    fn from(value: &OptionValue) -> Self {
        match value {
            OptionValue::String(value) => Self::String(value.clone()),
            OptionValue::Bytes(value) => {
                Self::Bytes(base64::engine::general_purpose::STANDARD.encode(value))
            }
            OptionValue::Int(value) => Self::Int(*value),
            OptionValue::Double(value) => Self::Double(*value),
            _ => unreachable!("ADBC OptionValue is non-exhaustive"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireOption {
    pub key: String,
    #[serde(flatten)]
    pub value: WireOptionValue,
}

pub fn encode_options(options: &[WireOption]) -> Result<String, ProtocolError> {
    Ok(serde_json::to_string(options)?)
}

pub fn decode_options(value: &str) -> Result<Vec<(String, OptionValue)>, ProtocolError> {
    serde_json::from_str::<Vec<WireOption>>(value)?
        .into_iter()
        .map(|option| Ok((option.key, option.value.into_adbc()?)))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireAdbcError {
    pub status: String,
    pub message: String,
    pub vendor_code: i32,
    pub sqlstate: Vec<i8>,
    pub details: Vec<(String, String)>,
}

impl From<&AdbcError> for WireAdbcError {
    fn from(error: &AdbcError) -> Self {
        Self {
            status: status_name(error.status).to_string(),
            message: error.message.clone(),
            vendor_code: error.vendor_code,
            sqlstate: error.sqlstate.to_vec(),
            details: error
                .details
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        base64::engine::general_purpose::STANDARD.encode(value),
                    )
                })
                .collect(),
        }
    }
}

impl WireAdbcError {
    pub fn into_adbc(self) -> AdbcError {
        let mut sqlstate = [0 as std::os::raw::c_char; 5];
        for (destination, source) in sqlstate.iter_mut().zip(self.sqlstate) {
            *destination = source as std::os::raw::c_char;
        }
        let details = self
            .details
            .into_iter()
            .map(|(key, value)| {
                let value = base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .unwrap_or_default();
                (key, value)
            })
            .collect::<Vec<_>>();
        AdbcError {
            message: self.message,
            status: status_from_name(&self.status),
            vendor_code: self.vendor_code,
            sqlstate,
            details: (!details.is_empty()).then_some(details),
        }
    }
}

pub fn status_name(status: Status) -> &'static str {
    match status {
        Status::Ok => "ok",
        Status::Unknown => "unknown",
        Status::NotImplemented => "not_implemented",
        Status::NotFound => "not_found",
        Status::AlreadyExists => "already_exists",
        Status::InvalidArguments => "invalid_arguments",
        Status::InvalidState => "invalid_state",
        Status::InvalidData => "invalid_data",
        Status::Integrity => "integrity",
        Status::Internal => "internal",
        Status::IO => "io",
        Status::Cancelled => "cancelled",
        Status::Timeout => "timeout",
        Status::Unauthenticated => "unauthenticated",
        Status::Unauthorized => "unauthorized",
    }
}

pub fn status_from_name(name: &str) -> Status {
    match name {
        "ok" => Status::Ok,
        "not_implemented" => Status::NotImplemented,
        "not_found" => Status::NotFound,
        "already_exists" => Status::AlreadyExists,
        "invalid_arguments" => Status::InvalidArguments,
        "invalid_state" => Status::InvalidState,
        "invalid_data" => Status::InvalidData,
        "integrity" => Status::Integrity,
        "internal" => Status::Internal,
        "io" => Status::IO,
        "cancelled" => Status::Cancelled,
        "timeout" => Status::Timeout,
        "unauthenticated" => Status::Unauthenticated,
        "unauthorized" => Status::Unauthorized,
        _ => Status::Unknown,
    }
}

pub fn empty_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "ok",
        DataType::Boolean,
        false,
    )]))
}

pub fn open_connection_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("target", DataType::Utf8, false),
        Field::new("database_options_json", DataType::Utf8, false),
        Field::new("connection_options_json", DataType::Utf8, false),
    ]))
}

pub fn session_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "session_id",
        DataType::Utf8,
        false,
    )]))
}

pub fn statement_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
    ]))
}

pub fn connection_args_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("args_json", DataType::Utf8, false),
    ]))
}

pub fn connection_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

pub fn connection_option_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_json", DataType::Utf8, false),
    ]))
}

pub fn connection_option_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
    ]))
}

pub fn set_sql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, false),
    ]))
}

pub fn statement_option_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_json", DataType::Utf8, false),
    ]))
}

pub fn statement_option_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
    ]))
}

pub fn statement_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

pub fn result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("result_id", DataType::Utf8, false),
    ]))
}

pub fn read_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("result_id", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
    ]))
}

pub fn read_result_batch_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("finished", DataType::Boolean, false),
        Field::new("payload", DataType::Binary, true),
    ]))
}

pub fn execute_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("result_id", DataType::Utf8, false),
        Field::new("rows_affected", DataType::Int64, true),
        Field::new("schema_ipc", DataType::Binary, false),
    ]))
}

pub fn update_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "rows_affected",
        DataType::Int64,
        true,
    )]))
}

pub fn value_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value_json",
        DataType::Utf8,
        false,
    )]))
}

pub fn schema_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "schema_ipc",
        DataType::Binary,
        false,
    )]))
}

pub fn partitions_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rows_affected", DataType::Int64, false),
        Field::new("schema_ipc", DataType::Binary, false),
        Field::new("partitions_json", DataType::Utf8, false),
    ]))
}

pub fn one_string(schema: SchemaRef, value: &str) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![value.to_string()]))],
    )
}

pub fn execute_response(
    result_id: &str,
    rows_affected: Option<i64>,
    schema: &Schema,
) -> Result<RecordBatch, ProtocolError> {
    let schema_ipc = encode_schema(schema)?;
    let mut ids = StringBuilder::new();
    ids.append_value(result_id);
    let mut rows = Int64Builder::new();
    rows.append_option(rows_affected);
    let mut schemas = BinaryBuilder::new();
    schemas.append_value(schema_ipc);
    Ok(RecordBatch::try_new(
        execute_response_schema(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(rows.finish()),
            Arc::new(schemas.finish()),
        ],
    )?)
}

pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>, ProtocolError> {
    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut arrow_ipc::writer::DictionaryTracker::new(false),
        &options,
    );
    Ok(encoded.ipc_message)
}

pub fn decode_schema(bytes: &[u8]) -> Result<Schema, ProtocolError> {
    let message = root_as_message(bytes).map_err(|_| ProtocolError::InvalidSchemaMessage)?;
    let ipc_schema = message
        .header_as_schema()
        .ok_or(ProtocolError::InvalidSchemaMessage)?;
    Ok(fb_to_schema(ipc_schema))
}

pub fn encode_batches<I>(schema: &Schema, batches: I) -> Result<Vec<u8>, ProtocolError>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    encode_batches_with_limit(schema, batches, MAX_BIND_STREAM_BYTES)
}

/// A writer which refuses an Arrow IPC write before growing past its budget.
///
/// Checking the `Vec` after `StreamWriter::write` is too late: a single large
/// batch has already caused the full allocation by then. Keep the first
/// attempted size separately so Arrow's wrapping of the I/O error can still be
/// translated into the protocol's stable size-limit error.
struct CappedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    rejected_actual: Arc<AtomicUsize>,
}

impl Write for CappedBuffer {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let actual = self.bytes.len().saturating_add(input.len());
        if actual > self.limit {
            self.rejected_actual.store(actual, Ordering::Relaxed);
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Arrow IPC stream exceeds configured byte limit",
            ));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_batches_with_limit<I>(
    schema: &Schema,
    batches: I,
    limit: usize,
) -> Result<Vec<u8>, ProtocolError>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    let rejected_actual = Arc::new(AtomicUsize::new(0));
    let buffer = CappedBuffer {
        bytes: Vec::new(),
        limit,
        rejected_actual: rejected_actual.clone(),
    };
    let map_arrow = |error| {
        let actual = rejected_actual.load(Ordering::Relaxed);
        if actual > limit {
            ProtocolError::StreamTooLarge { limit, actual }
        } else {
            ProtocolError::Arrow(error)
        }
    };
    let mut writer = StreamWriter::try_new(buffer, schema).map_err(&map_arrow)?;
    for batch in batches {
        writer.write(&batch?).map_err(&map_arrow)?;
    }
    writer.finish().map_err(&map_arrow)?;
    Ok(writer.into_inner().map_err(&map_arrow)?.bytes)
}

pub fn decode_batches(
    bytes: Vec<u8>,
) -> Result<StreamReader<std::io::Cursor<Vec<u8>>>, ProtocolError> {
    check_stream_size(bytes.len(), MAX_BIND_STREAM_BYTES)?;
    Ok(StreamReader::try_new(std::io::Cursor::new(bytes), None)?)
}

/// Decode a bind payload without first cloning its enclosing VGI binary
/// column. This is suitable when the reader is consumed before the request
/// handler returns (not for `bind_stream`, whose ADBC reader may be retained).
pub fn decode_batches_ref(
    bytes: &[u8],
) -> Result<StreamReader<std::io::Cursor<&[u8]>>, ProtocolError> {
    decode_batches_ref_with_limit(bytes, MAX_BIND_STREAM_BYTES)
}

pub fn decode_batches_ref_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<StreamReader<std::io::Cursor<&[u8]>>, ProtocolError> {
    check_stream_size(bytes.len(), limit)?;
    Ok(StreamReader::try_new(std::io::Cursor::new(bytes), None)?)
}

fn check_stream_size(actual: usize, limit: usize) -> Result<(), ProtocolError> {
    if actual > limit {
        return Err(ProtocolError::StreamTooLarge { limit, actual });
    }
    Ok(())
}

/// Encode one result batch for the byte-stream unary pull operation.
///
/// This deliberately does not apply the bind-stream buffering limit. The VGI
/// transport's own Arrow message ceiling remains the authoritative wire
/// safety bound.
pub fn encode_result_batch(batch: &RecordBatch) -> Result<Vec<u8>, ProtocolError> {
    let mut writer = StreamWriter::try_new(Vec::new(), batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

pub fn decode_result_batch(bytes: Vec<u8>) -> Result<RecordBatch, ProtocolError> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batch = reader.next().ok_or(ProtocolError::EmptyStream)??;
    if reader.next().is_some() {
        return Err(ProtocolError::Arrow(ArrowError::ParseError(
            "result payload contains more than one record batch".into(),
        )));
    }
    Ok(batch)
}

pub fn require_one_row(batch: &RecordBatch) -> Result<(), ProtocolError> {
    if batch.num_rows() != 1 {
        return Err(ProtocolError::RowCount(batch.num_rows()));
    }
    Ok(())
}

pub fn string_value<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a str, ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "utf8",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn int64_value(batch: &RecordBatch, name: &str) -> Result<i64, ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "int64",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn binary_value<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a [u8], ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "binary",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn batch_strings(batch: &RecordBatch) -> Result<HashMap<String, String>, ProtocolError> {
    require_one_row(batch)?;
    let mut values = HashMap::new();
    for field in batch.schema().fields() {
        if field.data_type() == &DataType::Utf8 {
            values.insert(
                field.name().clone(),
                string_value(batch, field.name())?.to_string(),
            );
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, Int64Array};

    fn binary_batch(payload_len: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let payload = vec![0x5a; payload_len];
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_vec(vec![payload.as_slice()]))],
        )
        .unwrap()
    }

    #[test]
    fn schema_round_trip() {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let decoded = decode_schema(&encode_schema(&schema).unwrap()).unwrap();
        assert_eq!(schema, decoded);
    }

    #[test]
    fn monolithic_bind_ceiling_reserves_binary_and_vgi_headroom() {
        assert_eq!(
            MAX_CONFIGURABLE_BIND_BYTES + BIND_ENVELOPE_HEADROOM_BYTES,
            i32::MAX as usize
        );
    }

    #[test]
    fn typed_options_round_trip() {
        let options = vec![
            WireOption {
                key: "s".into(),
                value: WireOptionValue::String("value".into()),
            },
            WireOption {
                key: "b".into(),
                value: WireOptionValue::Bytes(
                    base64::engine::general_purpose::STANDARD.encode([0, 1, 2]),
                ),
            },
            WireOption {
                key: "i".into(),
                value: WireOptionValue::Int(42),
            },
        ];
        let encoded = encode_options(&options).unwrap();
        let decoded = decode_options(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn record_batch_stream_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batches = vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap(),
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3]))])
                .unwrap(),
        ];
        let encoded = encode_batches(schema.as_ref(), batches.clone().into_iter().map(Ok)).unwrap();
        assert!(encoded.len() < MAX_BIND_STREAM_BYTES);
        let decoded = decode_batches(encoded)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(decoded, batches);
    }

    #[test]
    fn bind_arrow_ipc_cap_accepts_below_and_at_and_rejects_above() {
        // This uses a deliberately small synthetic cap so the always-on test
        // covers exact boundary semantics without a 64 MiB CI allocation.
        let batch = binary_batch(16 * 1024);
        let encoded =
            encode_batches_with_limit(batch.schema().as_ref(), [Ok(batch.clone())], usize::MAX)
                .unwrap();
        let exact = encoded.len();

        assert!(
            encode_batches_with_limit(batch.schema().as_ref(), [Ok(batch.clone())], exact + 1,)
                .is_ok()
        );
        assert!(
            encode_batches_with_limit(batch.schema().as_ref(), [Ok(batch.clone())], exact,).is_ok()
        );
        assert!(matches!(
            encode_batches_with_limit(batch.schema().as_ref(), [Ok(batch)], exact - 1),
            Err(ProtocolError::StreamTooLarge { limit, actual })
                if limit == exact - 1 && actual > limit
        ));

        assert!(check_stream_size(encoded.len(), encoded.len() + 1).is_ok());
        assert!(check_stream_size(encoded.len(), encoded.len()).is_ok());
        assert!(matches!(
            check_stream_size(encoded.len(), encoded.len() - 1),
            Err(ProtocolError::StreamTooLarge { .. })
        ));
    }

    #[test]
    fn borrowed_bind_decode_does_not_require_payload_clone() {
        let batch = binary_batch(1024);
        let encoded = encode_batches(batch.schema().as_ref(), [Ok(batch.clone())]).unwrap();
        let decoded = decode_batches_ref(&encoded)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(decoded, vec![batch]);
    }

    #[test]
    #[ignore = "allocates roughly 200 MiB transiently; run explicitly for the real 64 MiB cap"]
    fn heavy_real_bind_arrow_ipc_cap_below_at_and_above() {
        fn encoded_len(payload_len: usize) -> usize {
            let batch = binary_batch(payload_len);
            encode_batches_with_limit(batch.schema().as_ref(), [Ok(batch)], usize::MAX)
                .unwrap()
                .len()
        }

        // Arrow IPC stream sizes are discrete because buffers are aligned, so
        // there may be no valid stream whose length is exactly the byte cap.
        // Find the largest representable stream below it and the first above;
        // the exact raw-byte boundary itself is covered by check_stream_size.
        let probe_payload = MAX_BIND_STREAM_BYTES - 4096;
        let probe_len = encoded_len(probe_payload);
        let candidate = probe_payload + (MAX_BIND_STREAM_BYTES - probe_len);
        let mut below_payload = candidate;
        while encoded_len(below_payload) > MAX_BIND_STREAM_BYTES {
            below_payload -= 1;
        }
        let below_len = encoded_len(below_payload);
        assert!(below_len <= MAX_BIND_STREAM_BYTES);
        assert!(encoded_len(below_payload + 1) > MAX_BIND_STREAM_BYTES);

        let below = binary_batch(below_payload);
        assert!(encode_batches(below.schema().as_ref(), [Ok(below)]).is_ok());
        assert!(check_stream_size(MAX_BIND_STREAM_BYTES, MAX_BIND_STREAM_BYTES).is_ok());
        let above = binary_batch(below_payload + 1);
        assert!(matches!(
            encode_batches(above.schema().as_ref(), [Ok(above)]),
            Err(ProtocolError::StreamTooLarge { limit, actual })
                if limit == MAX_BIND_STREAM_BYTES && actual > limit
        ));
    }
}
