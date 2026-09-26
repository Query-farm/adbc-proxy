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

//! Typed unary results use stock VGI's binary, single-row Arrow IPC envelope.

use std::io::{Cursor, Write};
use std::sync::Arc;

use adbc_core::options::OptionValue;
use arrow_array::{Array, BinaryArray, BooleanArray, ListArray, RecordBatch, StructArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub use vgi_rpc::{Bytes, VgiArrow};

use crate::{ProtocolError, binary_value, require_one_row};

/// Hard allocation ceiling for nested control payloads. Transport limits may be lower.
pub const MAX_CONTROL_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct OkResponse {
    pub ok: bool,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct SessionResponse {
    pub session_id: String,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct StatementResponse {
    pub session_id: String,
    pub statement_id: String,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct ExecuteResponse {
    pub result_id: String,
    pub rows_affected: Option<i64>,
    pub schema_ipc: Bytes,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct SchemaResponse {
    pub schema_ipc: Bytes,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct UpdateResponse {
    pub rows_affected: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct PartitionsResponse {
    pub rows_affected: i64,
    pub schema_ipc: Bytes,
    pub partitions: Vec<Bytes>,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct ValueResponse {
    pub value: WireOptionValue,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct WireOptionValue {
    pub kind: String,
    pub string_value: Option<String>,
    pub bytes_value: Option<Bytes>,
    pub int_value: Option<i64>,
    pub double_value: Option<f64>,
}

impl From<&OptionValue> for WireOptionValue {
    fn from(value: &OptionValue) -> Self {
        let mut wire = Self {
            kind: String::new(),
            string_value: None,
            bytes_value: None,
            int_value: None,
            double_value: None,
        };
        match value {
            OptionValue::String(value) => {
                wire.kind = "string".into();
                wire.string_value = Some(value.clone());
            }
            OptionValue::Bytes(value) => {
                wire.kind = "bytes".into();
                wire.bytes_value = Some(Bytes(value.clone()));
            }
            OptionValue::Int(value) => {
                wire.kind = "int".into();
                wire.int_value = Some(*value);
            }
            OptionValue::Double(value) => {
                wire.kind = "double".into();
                wire.double_value = Some(*value);
            }
            _ => {
                wire.kind = "unsupported".into();
            }
        }
        wire
    }
}

impl WireOptionValue {
    pub fn into_adbc(self) -> Result<OptionValue, ProtocolError> {
        match (
            self.kind.as_str(),
            self.string_value,
            self.bytes_value,
            self.int_value,
            self.double_value,
        ) {
            ("string", Some(value), None, None, None) => Ok(OptionValue::String(value)),
            ("bytes", None, Some(value), None, None) => Ok(OptionValue::Bytes(value.0)),
            ("int", None, None, Some(value), None) => Ok(OptionValue::Int(value)),
            ("double", None, None, None, Some(value)) if value.is_finite() => {
                Ok(OptionValue::Double(value))
            }
            _ => Err(invalid(
                "option kind and populated value fields do not match",
            )),
        }
    }
}

fn invalid(message: &str) -> ProtocolError {
    ProtocolError::InvalidWire(message.into())
}

pub fn unary_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Binary,
        false,
    )]))
}

/// Fixed exchange schema carries dynamic Arrow batches without empty-schema turns.
pub fn bind_turn_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("batch_ipc", DataType::Binary, false),
        Field::new("finish", DataType::Boolean, false),
    ]))
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "Arrow IPC payload exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialize one complete IPC stream with a bounded sink, including its terminator.
pub fn encode_batch_ipc(batch: &RecordBatch, limit: usize) -> Result<Vec<u8>, ProtocolError> {
    if batch.get_array_memory_size() > limit {
        return Err(invalid("Arrow batch exceeds configured limit"));
    }
    let mut sink = BoundedBuffer {
        bytes: Vec::new(),
        limit,
    };
    {
        let mut writer = StreamWriter::try_new(&mut sink, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(sink.bytes)
}

/// Validate complete framing of the raw nested IPC stream before Arrow decoding.
fn validate_ipc(bytes: &[u8], limit: usize) -> Result<(), ProtocolError> {
    if bytes.len() > limit {
        return Err(invalid("Arrow IPC payload exceeds configured limit"));
    }
    let mut offset = 0usize;
    loop {
        let prefix = bytes
            .get(offset..offset.saturating_add(4))
            .ok_or_else(|| invalid("truncated IPC prefix"))?;
        let mut length = i32::from_le_bytes(prefix.try_into().unwrap());
        offset += 4;
        if length == -1 {
            let prefix = bytes
                .get(offset..offset.saturating_add(4))
                .ok_or_else(|| invalid("truncated IPC continuation"))?;
            length = i32::from_le_bytes(prefix.try_into().unwrap());
            offset += 4;
        }
        if length == 0 {
            return if offset == bytes.len() {
                Ok(())
            } else {
                Err(invalid("trailing IPC bytes"))
            };
        }
        let length = usize::try_from(length).map_err(|_| invalid("invalid IPC metadata length"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("IPC length overflow"))?;
        let metadata = bytes
            .get(offset..end)
            .ok_or_else(|| invalid("truncated IPC metadata"))?;
        let message =
            arrow_ipc::root_as_message(metadata).map_err(|_| invalid("invalid IPC metadata"))?;
        let body = usize::try_from(message.bodyLength())
            .map_err(|_| invalid("invalid IPC body length"))?;
        offset = end
            .checked_add(body)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| invalid("truncated IPC body"))?;
    }
}

/// Decode exactly one batch, rejecting extra batches, truncation and trailing streams.
pub fn decode_batch_ipc(bytes: &[u8], limit: usize) -> Result<RecordBatch, ProtocolError> {
    validate_ipc(bytes, limit)?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let batch = reader
        .next()
        .ok_or_else(|| invalid("IPC stream must contain one batch"))??;
    if reader.next().transpose()?.is_some() {
        return Err(invalid("IPC stream contains multiple batches"));
    }
    if batch.get_array_memory_size() > limit {
        return Err(invalid("decoded Arrow batch exceeds configured limit"));
    }
    Ok(batch)
}

fn validate_array(array: &dyn Array, nullable: bool) -> Result<(), ProtocolError> {
    if !nullable && array.null_count() != 0 {
        return Err(invalid("null in required response field"));
    }
    if let Some(record) = array.as_any().downcast_ref::<StructArray>() {
        for (field, child) in record.fields().iter().zip(record.columns()) {
            validate_array(child.as_ref(), field.is_nullable())?;
        }
    }
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        // Python's list[bytes] schema allows null elements, but the public type does not.
        validate_array(list.values().as_ref(), false)?;
    }
    Ok(())
}

/// Encode a typed response exactly as ArrowSerializableDataclass on stock Python VGI.
pub fn encode_response<T: VgiArrow>(value: T, limit: usize) -> Result<RecordBatch, ProtocolError> {
    let array = T::build_singleton(value).map_err(|error| invalid(&error.to_string()))?;
    let record = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid("response type must be a record"))?;
    validate_array(record, false)?;
    let bytes = encode_batch_ipc(&RecordBatch::from(record.clone()), limit)?;
    Ok(RecordBatch::try_new(
        unary_response_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()]))],
    )?)
}

/// Decode into the declared response struct, with exact schema and null checks.
pub fn decode_response<T: VgiArrow>(outer: &RecordBatch, limit: usize) -> Result<T, ProtocolError> {
    if outer.schema().fields() != unary_response_schema().fields() {
        return Err(invalid("unexpected unary response schema"));
    }
    let batch = decode_batch_ipc(binary_value(outer, "result")?, limit)?;
    require_one_row(&batch)?;
    let record = StructArray::from(batch);
    if record.data_type() != &T::arrow_data_type() {
        return Err(invalid("unexpected typed response schema"));
    }
    validate_array(&record, false)?;
    T::read(&record, 0).map_err(|error| invalid(&error.to_string()))
}

pub fn encode_bind_turn(
    batch: Option<&RecordBatch>,
    limit: usize,
) -> Result<RecordBatch, ProtocolError> {
    let payload = batch
        .map(|batch| encode_batch_ipc(batch, limit))
        .transpose()?
        .unwrap_or_default();
    Ok(RecordBatch::try_new(
        bind_turn_schema(),
        vec![
            Arc::new(BinaryArray::from_vec(vec![payload.as_slice()])),
            Arc::new(BooleanArray::from(vec![batch.is_none()])),
        ],
    )?)
}

pub fn decode_bind_turn(
    turn: &RecordBatch,
    limit: usize,
) -> Result<Option<RecordBatch>, ProtocolError> {
    require_one_row(turn)?;
    if turn.schema().fields() != bind_turn_schema().fields() {
        return Err(invalid("unexpected bind turn schema"));
    }
    let payload = binary_value(turn, "batch_ipc")?;
    let finish = turn
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| invalid("invalid bind finish field"))?;
    if finish.is_null(0) {
        return Err(invalid("null bind finish field"));
    }
    if finish.value(0) {
        if !payload.is_empty() {
            return Err(invalid("bind finish payload must be empty"));
        }
        Ok(None)
    } else {
        decode_batch_ipc(payload, limit).map(Some)
    }
}
