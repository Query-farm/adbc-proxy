//! Versioned Arrow schemas and wire values for ADBC-over-VGI.

use std::collections::HashMap;
use std::sync::Arc;

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
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    for batch in batches {
        writer.write(&batch?)?;
        let actual = writer.get_ref().len();
        if actual > MAX_BIND_STREAM_BYTES {
            return Err(ProtocolError::StreamTooLarge {
                limit: MAX_BIND_STREAM_BYTES,
                actual,
            });
        }
    }
    writer.finish()?;
    let bytes = writer.into_inner()?;
    if bytes.len() > MAX_BIND_STREAM_BYTES {
        return Err(ProtocolError::StreamTooLarge {
            limit: MAX_BIND_STREAM_BYTES,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

pub fn decode_batches(
    bytes: Vec<u8>,
) -> Result<StreamReader<std::io::Cursor<Vec<u8>>>, ProtocolError> {
    if bytes.len() > MAX_BIND_STREAM_BYTES {
        return Err(ProtocolError::StreamTooLarge {
            limit: MAX_BIND_STREAM_BYTES,
            actual: bytes.len(),
        });
    }
    Ok(StreamReader::try_new(std::io::Cursor::new(bytes), None)?)
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
    use arrow_array::Int64Array;

    #[test]
    fn schema_round_trip() {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let decoded = decode_schema(&encode_schema(&schema).unwrap()).unwrap();
        assert_eq!(schema, decoded);
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
}
