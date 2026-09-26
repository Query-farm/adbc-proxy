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

use adbc_core::options::OptionValue;
use arrow_array::{
    ArrayRef, BinaryArray, RecordBatch, StructArray,
    builder::{Int64Builder, ListBuilder},
};
use grainlift_protocol::*;
use std::sync::Arc;

fn round_trip<T: RequestRecord + Clone + PartialEq + std::fmt::Debug>(value: T) {
    let batch = encode_request(value.clone(), MAX_CONTROL_BYTES).unwrap();
    assert_eq!(batch.schema(), typed_request_schema());
    assert_eq!(
        decode_request::<T>(&batch, MAX_CONTROL_BYTES).unwrap(),
        value
    );
}

fn unchecked<T: VgiArrow>(value: T) -> RecordBatch {
    let bytes = encode_record_ipc(value, MAX_CONTROL_BYTES).unwrap();
    RecordBatch::try_new(
        typed_request_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()]))],
    )
    .unwrap()
}

#[test]
fn named_request_round_trips_preserve_filters_and_typed_options() {
    let options = [
        OptionValue::String("".into()),
        OptionValue::Bytes(vec![0, 255]),
        OptionValue::Int(i64::MIN),
        OptionValue::Double(1.25),
    ];
    let options: Vec<_> = options
        .into_iter()
        .enumerate()
        .map(|(index, value)| NamedOption {
            key: format!("option{index}"),
            value: WireOptionValue::from(&value),
        })
        .collect();
    round_trip(OpenConnectionRequest {
        target: "target".into(),
        database_options: options.clone(),
        connection_options: options,
    });
    let value = WireOptionValue::from(&OptionValue::Int(i64::MAX));
    round_trip(SetConnectionOptionRequest {
        session_id: "session".into(),
        key: "key".into(),
        value: value.clone(),
    });
    round_trip(SetStatementOptionRequest {
        session_id: "session".into(),
        statement_id: "statement".into(),
        key: "key".into(),
        value,
    });
    for codes in [None, Some(vec![]), Some(vec![0, 9999, u32::MAX.into()])] {
        round_trip(GetInfoRequest {
            session_id: "session".into(),
            codes,
        });
    }
    for types in [None, Some(vec![]), Some(vec!["".into(), "TABLE".into()])] {
        round_trip(GetObjectsRequest {
            session_id: "session".into(),
            depth: 0,
            catalog: None,
            db_schema: Some("".into()),
            table_name: Some("%_".into()),
            table_types: types,
            column_name: None,
        });
    }
    round_trip(GetTableSchemaRequest {
        session_id: "session".into(),
        catalog: None,
        db_schema: Some("".into()),
        table_name: "".into(),
    });
    for approximate in [false, true] {
        round_trip(GetStatisticsRequest {
            session_id: "session".into(),
            catalog: Some("".into()),
            db_schema: None,
            table_name: Some("%".into()),
            approximate,
        });
    }
}

#[test]
fn semantic_validation_precedes_request_use() {
    for code in [-1, i64::from(u32::MAX) + 1] {
        let value = GetInfoRequest {
            session_id: "session".into(),
            codes: Some(vec![code]),
        };
        assert!(encode_request(value.clone(), MAX_CONTROL_BYTES).is_err());
        assert!(decode_request::<GetInfoRequest>(&unchecked(value), MAX_CONTROL_BYTES).is_err());
    }
    for depth in [-1, 4, i64::MAX] {
        assert!(
            decode_request::<GetObjectsRequest>(
                &unchecked(GetObjectsRequest {
                    session_id: "session".into(),
                    depth,
                    catalog: None,
                    db_schema: None,
                    table_name: None,
                    table_types: None,
                    column_name: None
                }),
                MAX_CONTROL_BYTES
            )
            .is_err()
        );
    }
    for key in ["", "nul\0key"] {
        assert!(
            decode_request::<SetConnectionOptionRequest>(
                &unchecked(SetConnectionOptionRequest {
                    session_id: "session".into(),
                    key: key.into(),
                    value: WireOptionValue::from(&OptionValue::Int(1))
                }),
                MAX_CONTROL_BYTES
            )
            .is_err()
        );
    }
    for handle in ["", "nul\0handle"] {
        assert!(
            encode_request(
                GetInfoRequest {
                    session_id: handle.into(),
                    codes: None
                },
                MAX_CONTROL_BYTES
            )
            .is_err()
        );
    }
    assert!(
        encode_request(
            GetTableSchemaRequest {
                session_id: "session".into(),
                catalog: Some("bad\0catalog".into()),
                db_schema: None,
                table_name: "table".into()
            },
            MAX_CONTROL_BYTES
        )
        .is_err()
    );
    let option = NamedOption {
        key: "duplicate".into(),
        value: WireOptionValue::from(&OptionValue::Int(1)),
    };
    assert!(
        decode_request::<OpenConnectionRequest>(
            &unchecked(OpenConnectionRequest {
                target: "target".into(),
                database_options: vec![option.clone(), option],
                connection_options: vec![]
            }),
            MAX_CONTROL_BYTES
        )
        .is_err()
    );
    let mut value = WireOptionValue::from(&OptionValue::Double(f64::INFINITY));
    value.int_value = Some(1);
    assert!(
        decode_request::<SetStatementOptionRequest>(
            &unchecked(SetStatementOptionRequest {
                session_id: "session".into(),
                statement_id: "statement".into(),
                key: "key".into(),
                value
            }),
            MAX_CONTROL_BYTES
        )
        .is_err()
    );
}

#[test]
fn request_decoder_rejects_null_list_items_and_incorrect_record_schema() {
    let value = GetInfoRequest {
        session_id: "session".into(),
        codes: Some(vec![]),
    };
    let array = GetInfoRequest::build_singleton(value.clone()).unwrap();
    let record = array.as_any().downcast_ref::<StructArray>().unwrap();
    let mut list = ListBuilder::new(Int64Builder::new());
    list.values().append_null();
    list.append(true);
    let columns = vec![
        record.column(0).clone(),
        Arc::new(list.finish()) as ArrayRef,
    ];
    let record = StructArray::new(record.fields().clone(), columns, None);
    let bytes = encode_batch_ipc(&RecordBatch::from(record), MAX_CONTROL_BYTES).unwrap();
    let outer = RecordBatch::try_new(
        typed_request_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()]))],
    )
    .unwrap();
    assert!(decode_request::<GetInfoRequest>(&outer, MAX_CONTROL_BYTES).is_err());
    assert!(decode_request::<GetTableSchemaRequest>(&unchecked(value), MAX_CONTROL_BYTES).is_err());
}

#[test]
fn request_envelope_size_is_checked_below_at_and_above_boundary() {
    let request = GetInfoRequest {
        session_id: "session".into(),
        codes: None,
    };
    let outer = encode_request(request.clone(), MAX_CONTROL_BYTES).unwrap();
    let size = encode_batch_ipc(&outer, MAX_CONTROL_BYTES).unwrap().len();
    assert!(encode_request(request.clone(), size - 1).is_err());
    for limit in [size, size + 1] {
        assert_eq!(encode_request(request.clone(), limit).unwrap(), outer);
    }
    let bytes = binary_value(&outer, "request").unwrap();
    assert!(decode_request::<GetInfoRequest>(&outer, bytes.len() - 1).is_err());
}

#[test]
fn nonfinite_request_options_preserve_their_bits() {
    for value in [
        f64::from_bits(0x7ff8000000000077),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let input = SetConnectionOptionRequest {
            session_id: "session".into(),
            key: "double".into(),
            value: WireOptionValue::from(&OptionValue::Double(value)),
        };
        let outer = encode_request(input, MAX_CONTROL_BYTES).unwrap();
        let output: SetConnectionOptionRequest = decode_request(&outer, MAX_CONTROL_BYTES).unwrap();
        assert_eq!(
            output.value.double_value.unwrap().to_bits(),
            value.to_bits()
        );
    }
}
