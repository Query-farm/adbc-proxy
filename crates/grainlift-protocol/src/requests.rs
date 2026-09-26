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

//! Named application requests, encoded as stock VGI dataclass binary parameters.

use crate::responses::{decode_record, encode_record, envelope_schema};
use crate::{ProtocolError, WireOptionValue};
use adbc_core::options::OptionValue;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::collections::HashSet;
use vgi_rpc::VgiArrow;

#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct NamedOption {
    pub key: String,
    pub value: WireOptionValue,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct OpenConnectionRequest {
    pub target: String,
    pub database_options: Vec<NamedOption>,
    pub connection_options: Vec<NamedOption>,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct SetConnectionOptionRequest {
    pub session_id: String,
    pub key: String,
    pub value: WireOptionValue,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct SetStatementOptionRequest {
    pub session_id: String,
    pub statement_id: String,
    pub key: String,
    pub value: WireOptionValue,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct GetInfoRequest {
    pub session_id: String,
    pub codes: Option<Vec<i64>>,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct GetObjectsRequest {
    pub session_id: String,
    pub depth: i64,
    pub catalog: Option<String>,
    pub db_schema: Option<String>,
    pub table_name: Option<String>,
    pub table_types: Option<Vec<String>>,
    pub column_name: Option<String>,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct GetTableSchemaRequest {
    pub session_id: String,
    pub catalog: Option<String>,
    pub db_schema: Option<String>,
    pub table_name: String,
}
#[derive(Debug, Clone, PartialEq, VgiArrow)]
pub struct GetStatisticsRequest {
    pub session_id: String,
    pub catalog: Option<String>,
    pub db_schema: Option<String>,
    pub table_name: Option<String>,
    pub approximate: bool,
}

fn invalid(message: &str) -> ProtocolError {
    ProtocolError::InvalidWire(message.into())
}

pub fn validate_text(value: &str) -> Result<(), ProtocolError> {
    if value.contains('\0') {
        Err(invalid("text must not contain NUL"))
    } else {
        Ok(())
    }
}

pub fn validate_handle(value: &str) -> Result<(), ProtocolError> {
    validate_text(value)?;
    if value.is_empty() {
        Err(invalid("handle or option key must not be empty"))
    } else {
        Ok(())
    }
}

fn filters(values: &[Option<&str>]) -> Result<(), ProtocolError> {
    for value in values.iter().flatten() {
        validate_text(value)?;
    }
    Ok(())
}

/// Validate domain constraints before any backend or registry mutation.
pub trait RequestRecord: VgiArrow {
    fn validate(&self) -> Result<(), ProtocolError>;
}

fn validate_options(options: &[NamedOption]) -> Result<(), ProtocolError> {
    let mut keys = HashSet::new();
    for option in options {
        validate_handle(&option.key)?;
        if !keys.insert(option.key.as_str()) {
            return Err(invalid("duplicate initial option key"));
        }
        option.value.validate()?;
    }
    Ok(())
}

pub fn named_options_into_adbc(
    options: Vec<NamedOption>,
) -> Result<Vec<(String, OptionValue)>, ProtocolError> {
    validate_options(&options)?;
    options
        .into_iter()
        .map(|option| Ok((option.key, option.value.into_adbc()?)))
        .collect()
}

impl RequestRecord for OpenConnectionRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.target)?;
        validate_options(&self.database_options)?;
        validate_options(&self.connection_options)
    }
}
impl RequestRecord for SetConnectionOptionRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        validate_handle(&self.key)?;
        self.value.validate()
    }
}
impl RequestRecord for SetStatementOptionRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        validate_handle(&self.statement_id)?;
        validate_handle(&self.key)?;
        self.value.validate()
    }
}
impl RequestRecord for GetInfoRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        if self
            .codes
            .as_ref()
            .is_some_and(|codes| codes.iter().any(|code| u32::try_from(*code).is_err()))
        {
            return Err(invalid("info codes must fit uint32"));
        }
        Ok(())
    }
}
impl RequestRecord for GetObjectsRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        filters(&[
            self.catalog.as_deref(),
            self.db_schema.as_deref(),
            self.table_name.as_deref(),
            self.column_name.as_deref(),
        ])?;
        if let Some(values) = &self.table_types {
            for value in values {
                validate_text(value)?;
            }
        }
        if !(0..=3).contains(&self.depth) {
            return Err(invalid("object depth must be one of 0, 1, 2, 3"));
        }
        Ok(())
    }
}
impl RequestRecord for GetTableSchemaRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        filters(&[
            self.catalog.as_deref(),
            self.db_schema.as_deref(),
            Some(&self.table_name),
        ])
    }
}
impl RequestRecord for GetStatisticsRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_handle(&self.session_id)?;
        filters(&[
            self.catalog.as_deref(),
            self.db_schema.as_deref(),
            self.table_name.as_deref(),
        ])
    }
}

pub fn typed_request_schema() -> SchemaRef {
    envelope_schema("request")
}

pub fn encode_request<T: RequestRecord>(
    value: T,
    limit: usize,
) -> Result<RecordBatch, ProtocolError> {
    value.validate()?;
    let batch = encode_record(value, "request", limit)?;
    crate::encode_batch_ipc(&batch, limit)?;
    Ok(batch)
}

pub fn decode_request<T: RequestRecord>(
    outer: &RecordBatch,
    limit: usize,
) -> Result<T, ProtocolError> {
    let value: T = decode_record(outer, "request", limit)?;
    value.validate()?;
    Ok(value)
}
