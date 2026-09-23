use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::Error as AdbcError;
use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
use adbc_proxy_protocol as protocol;
use arrow_array::{
    BinaryArray, BooleanArray, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use serde::{Deserialize, Serialize};
use vgi_rpc::server::{MethodInfo, MethodType, RpcServer, StateDecoder};
use vgi_rpc::stream::{OutputCollector, ProducerState, StreamResult, StreamStateKind};
use vgi_rpc::{CallContext, Request, RpcError};

use crate::session::SessionManager;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ResultCursor {
    session_id: String,
    result_id: String,
    sequence: i64,
}

struct ResultProducer {
    manager: Arc<SessionManager>,
    cursor: ResultCursor,
}

impl ProducerState for ResultProducer {
    fn produce(&mut self, out: &mut OutputCollector, ctx: &CallContext) -> vgi_rpc::Result<()> {
        let principal = self.manager.principal(&ctx.auth)?;
        let session = self
            .manager
            .get(&self.cursor.session_id, &principal)
            .map_err(adbc_rpc_error)?;
        let result = session
            .result(&self.cursor.result_id)
            .map_err(adbc_rpc_error)?;
        let batch = result
            .lock()
            .map_err(|_| RpcError::runtime_error("result is poisoned"))?
            .next(self.cursor.sequence)
            .map_err(adbc_rpc_error)?;
        match batch {
            Some(batch) => {
                out.emit(batch)?;
                self.cursor.sequence += 1;
            }
            None => out.finish(),
        }
        Ok(())
    }

    fn on_cancel(&mut self, ctx: &CallContext) {
        if let Ok(principal) = self.manager.principal(&ctx.auth)
            && let Ok(session) = self.manager.get(&self.cursor.session_id, &principal)
        {
            let _ = session.close_result(&self.cursor.result_id);
        }
    }

    fn encode_state(&self) -> vgi_rpc::Result<Vec<u8>> {
        serde_json::to_vec(&self.cursor)
            .map_err(|error| RpcError::runtime_error(format!("encode result cursor: {error}")))
    }
}

pub fn build_server(manager: Arc<SessionManager>, server_id: String) -> RpcServer {
    build_server_with_max_bind(manager, server_id, protocol::MAX_BIND_STREAM_BYTES)
}

pub fn build_server_with_max_bind(
    manager: Arc<SessionManager>,
    server_id: String,
    max_bind_bytes: usize,
) -> RpcServer {
    let hook = vgi_rpc::OtelHook::new(vgi_rpc::OtelConfig {
        service_name: "adbc-proxy".to_string(),
        record_exceptions: false,
    });
    let mut server = RpcServer::builder()
        .server_id(server_id)
        .server_version(env!("CARGO_PKG_VERSION"))
        .protocol_name(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .with_hook(hook)
        .build();

    register_open_connection(&mut server, manager.clone());
    register_session_operations(&mut server, manager.clone());
    register_connection_surface(&mut server, manager.clone());
    register_statement_operations(&mut server, manager.clone(), max_bind_bytes);
    register_result_operations(&mut server, manager);
    server
}

#[derive(Deserialize)]
struct InfoArgs {
    codes: Option<Vec<u32>>,
}

#[derive(Deserialize)]
struct ObjectsArgs {
    depth: i32,
    catalog: Option<String>,
    db_schema: Option<String>,
    table_name: Option<String>,
    table_type: Option<Vec<String>>,
    column_name: Option<String>,
}

#[derive(Deserialize)]
struct TableSchemaArgs {
    catalog: Option<String>,
    db_schema: Option<String>,
    table_name: String,
}

#[derive(Deserialize)]
struct StatisticsArgs {
    catalog: Option<String>,
    db_schema: Option<String>,
    table_name: Option<String>,
    approximate: bool,
}

fn register_open_connection(server: &mut RpcServer, manager: Arc<SessionManager>) {
    server.register(
        MethodInfo::unary(
            protocol::method::OPEN_CONNECTION,
            protocol::open_connection_schema(),
            protocol::session_schema(),
            move |request, ctx| {
                let principal = manager.principal(&ctx.auth)?;
                let target = string(request, "target")?;
                let database_options =
                    protocol::decode_options(string(request, "database_options_json")?)
                        .map_err(protocol_rpc_error)?;
                let connection_options =
                    protocol::decode_options(string(request, "connection_options_json")?)
                        .map_err(protocol_rpc_error)?;
                let session_id = manager
                    .open(principal, target, database_options, connection_options)
                    .map_err(adbc_rpc_error)?;
                Ok(Some(
                    RecordBatch::try_new(
                        protocol::session_schema(),
                        vec![Arc::new(StringArray::from(vec![session_id]))],
                    )
                    .map_err(arrow_rpc_error)?,
                ))
            },
        )
        .doc("Open a server-side ADBC connection to an authorized target"),
    );
}

fn register_session_operations(server: &mut RpcServer, manager: Arc<SessionManager>) {
    type SessionOperation = fn(&crate::session::Session) -> Result<(), AdbcError>;
    let operations: [(&str, SessionOperation); 2] = [
        (protocol::method::COMMIT, crate::session::Session::commit),
        (
            protocol::method::ROLLBACK,
            crate::session::Session::rollback,
        ),
    ];
    for (name, operation) in operations {
        let manager = manager.clone();
        server.register(MethodInfo::unary(
            name,
            protocol::session_schema(),
            protocol::empty_response_schema(),
            move |request, ctx| {
                let (session, _) = get_session(&manager, request, ctx)?;
                operation(&session).map_err(adbc_rpc_error)?;
                Ok(Some(ok_batch()?))
            },
        ));
    }

    let cancel_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CANCEL_CONNECTION,
        protocol::session_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&cancel_manager, request, ctx)?;
            session.cancel_connection().map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let close_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_CONNECTION,
        protocol::session_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let principal = close_manager.principal(&ctx.auth)?;
            close_manager
                .close(string(request, "session_id")?, &principal)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let statement_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::NEW_STATEMENT,
        protocol::session_schema(),
        protocol::statement_schema(),
        move |request, ctx| {
            let (session, session_id) = get_session(&statement_manager, request, ctx)?;
            let statement_id = session.new_statement().map_err(adbc_rpc_error)?;
            Ok(Some(
                RecordBatch::try_new(
                    protocol::statement_schema(),
                    vec![
                        Arc::new(StringArray::from(vec![session_id])),
                        Arc::new(StringArray::from(vec![statement_id])),
                    ],
                )
                .map_err(arrow_rpc_error)?,
            ))
        },
    ));
}

fn register_connection_surface(server: &mut RpcServer, manager: Arc<SessionManager>) {
    let set_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_CONNECTION_OPTION,
        protocol::connection_option_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&set_manager, request, ctx)?;
            let key = string(request, "key")?;
            let value = decode_option_value(string(request, "value_json")?)?;
            session
                .with_connection(|connection| connection.set_option(key, value))
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let get_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_CONNECTION_OPTION,
        protocol::connection_option_key_schema(),
        protocol::value_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&get_manager, request, ctx)?;
            let key = string(request, "key")?;
            let value_type = string(request, "value_type")?;
            let value = session
                .with_connection(|connection| match value_type {
                    "string" => connection.get_option_string(key).map(OptionValue::String),
                    "bytes" => connection.get_option_bytes(key).map(OptionValue::Bytes),
                    "int" => connection.get_option_int(key).map(OptionValue::Int),
                    "double" => connection.get_option_double(key).map(OptionValue::Double),
                    _ => Err(AdbcError::with_message_and_status(
                        "unknown option value type",
                        adbc_core::error::Status::InvalidArguments,
                    )),
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(value_response(&value)?))
        },
    ));

    let info_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_INFO,
        protocol::connection_args_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&info_manager, request, ctx)?;
            let args: InfoArgs = json_args(request)?;
            let codes = args.codes.map(|values| {
                values
                    .into_iter()
                    .map(InfoCode::from)
                    .collect::<HashSet<_>>()
            });
            let reader = session
                .with_connection(|connection| connection.get_info(codes))
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));

    let objects_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_OBJECTS,
        protocol::connection_args_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&objects_manager, request, ctx)?;
            let args: ObjectsArgs = json_args(request)?;
            let depth = ObjectDepth::try_from(args.depth).map_err(adbc_rpc_error)?;
            let table_type = args
                .table_type
                .as_ref()
                .map(|values| values.iter().map(String::as_str).collect());
            let reader = session
                .with_connection(|connection| {
                    connection.get_objects(
                        depth,
                        args.catalog.as_deref(),
                        args.db_schema.as_deref(),
                        args.table_name.as_deref(),
                        table_type,
                        args.column_name.as_deref(),
                    )
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));

    let table_schema_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_TABLE_SCHEMA,
        protocol::connection_args_schema(),
        protocol::schema_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&table_schema_manager, request, ctx)?;
            let args: TableSchemaArgs = json_args(request)?;
            let schema = session
                .with_connection(|connection| {
                    connection.get_table_schema(
                        args.catalog.as_deref(),
                        args.db_schema.as_deref(),
                        &args.table_name,
                    )
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(schema_response(&schema)?))
        },
    ));

    let table_types_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_TABLE_TYPES,
        protocol::session_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&table_types_manager, request, ctx)?;
            let reader = session
                .with_connection(|connection| connection.get_table_types())
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));

    let statistic_names_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_STATISTIC_NAMES,
        protocol::session_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&statistic_names_manager, request, ctx)?;
            let reader = session
                .with_connection(|connection| connection.get_statistic_names())
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));

    let statistics_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_STATISTICS,
        protocol::connection_args_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&statistics_manager, request, ctx)?;
            let args: StatisticsArgs = json_args(request)?;
            let reader = session
                .with_connection(|connection| {
                    connection.get_statistics(
                        args.catalog.as_deref(),
                        args.db_schema.as_deref(),
                        args.table_name.as_deref(),
                        args.approximate,
                    )
                })
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));

    let partition_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::READ_PARTITION,
        protocol::connection_binary_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&partition_manager, request, ctx)?;
            let partition = binary(request, "payload")?;
            let reader = session
                .with_connection(|connection| connection.read_partition(partition))
                .map_err(adbc_rpc_error)?;
            Ok(Some(insert_reader_response(&session, reader)?))
        },
    ));
}

fn register_statement_operations(
    server: &mut RpcServer,
    manager: Arc<SessionManager>,
    max_bind_bytes: usize,
) {
    let set_option_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_STATEMENT_OPTION,
        protocol::statement_option_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&set_option_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            let key = string(request, "key")?;
            let value = decode_option_value(string(request, "value_json")?)?;
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .set_option(key, value)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let get_option_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_STATEMENT_OPTION,
        protocol::statement_option_key_schema(),
        protocol::value_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&get_option_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            let key = string(request, "key")?;
            let value_type = string(request, "value_type")?;
            let statement = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?;
            let value = match value_type {
                "string" => statement.get_option_string(key).map(OptionValue::String),
                "bytes" => statement.get_option_bytes(key).map(OptionValue::Bytes),
                "int" => statement.get_option_int(key).map(OptionValue::Int),
                "double" => statement.get_option_double(key).map(OptionValue::Double),
                _ => Err(AdbcError::with_message_and_status(
                    "unknown option value type",
                    adbc_core::error::Status::InvalidArguments,
                )),
            }
            .map_err(adbc_rpc_error)?;
            Ok(Some(value_response(&value)?))
        },
    ));

    let set_sql_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_SQL_QUERY,
        protocol::set_sql_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&set_sql_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .set_sql_query(string(request, "sql")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let substrait_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::SET_SUBSTRAIT_PLAN,
        protocol::statement_binary_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&substrait_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .set_substrait_plan(binary(request, "payload")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let prepare_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::PREPARE,
        protocol::statement_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&prepare_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .prepare()
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let bind_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::BIND,
        protocol::statement_binary_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&bind_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            let mut reader = protocol::decode_batches_ref_with_limit(
                binary(request, "payload")?,
                max_bind_bytes,
            )
            .map_err(protocol_rpc_error)?;
            let batch = reader
                .next()
                .ok_or(protocol::ProtocolError::EmptyStream)
                .map_err(protocol_rpc_error)?
                .map_err(arrow_rpc_error)?;
            if reader.next().is_some() {
                return Err(RpcError::value_error(
                    "bind payload must contain exactly one record batch",
                ));
            }
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .bind(batch)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let bind_stream_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::BIND_STREAM,
        protocol::statement_binary_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&bind_stream_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            // An ADBC implementation may retain a bind-stream reader after
            // this handler returns, so it must be owned. Decode eagerly from
            // the borrowed request bytes, then retain only the decoded Arrow
            // arrays rather than cloning and retaining the entire IPC payload.
            let reader = protocol::decode_batches_ref_with_limit(
                binary(request, "payload")?,
                max_bind_bytes,
            )
            .map_err(protocol_rpc_error)?;
            let schema = reader.schema();
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(arrow_rpc_error)?;
            let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .bind_stream(Box::new(reader))
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let cancel_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CANCEL_STATEMENT,
        protocol::statement_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&cancel_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            statement.cancel.try_cancel().map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let execute_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE,
        protocol::statement_schema(),
        protocol::execute_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&execute_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?.to_string();
            session
                .invalidate_statement_results(&statement_id)
                .map_err(adbc_rpc_error)?;
            let statement = session.statement(&statement_id).map_err(adbc_rpc_error)?;
            let reader = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .execute()
                .map_err(adbc_rpc_error)?;
            let (result_id, schema) = session
                .insert_statement_result(&statement_id, reader)
                .map_err(adbc_rpc_error)?;
            Ok(Some(
                protocol::execute_response(&result_id, None, schema.as_ref())
                    .map_err(protocol_rpc_error)?,
            ))
        },
    ));

    let update_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_UPDATE,
        protocol::statement_schema(),
        protocol::update_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&update_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            session
                .invalidate_statement_results(statement_id)
                .map_err(adbc_rpc_error)?;
            let statement = session.statement(statement_id).map_err(adbc_rpc_error)?;
            let affected = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .execute_update()
                .map_err(adbc_rpc_error)?;
            Ok(Some(
                RecordBatch::try_new(
                    protocol::update_response_schema(),
                    vec![Arc::new(Int64Array::from(vec![affected]))],
                )
                .map_err(arrow_rpc_error)?,
            ))
        },
    ));

    let schema_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_SCHEMA,
        protocol::statement_schema(),
        protocol::schema_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&schema_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            session
                .invalidate_statement_results(statement_id)
                .map_err(adbc_rpc_error)?;
            let statement = session.statement(statement_id).map_err(adbc_rpc_error)?;
            let schema = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .execute_schema()
                .map_err(adbc_rpc_error)?;
            Ok(Some(schema_response(&schema)?))
        },
    ));

    let parameter_schema_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::GET_PARAMETER_SCHEMA,
        protocol::statement_schema(),
        protocol::schema_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&parameter_schema_manager, request, ctx)?;
            let statement = session
                .statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            let schema = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .get_parameter_schema()
                .map_err(adbc_rpc_error)?;
            Ok(Some(schema_response(&schema)?))
        },
    ));

    let partitions_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::EXECUTE_PARTITIONS,
        protocol::statement_schema(),
        protocol::partitions_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&partitions_manager, request, ctx)?;
            let statement_id = string(request, "statement_id")?;
            session
                .invalidate_statement_results(statement_id)
                .map_err(adbc_rpc_error)?;
            let statement = session.statement(statement_id).map_err(adbc_rpc_error)?;
            let result = statement
                .statement
                .lock()
                .map_err(|_| RpcError::runtime_error("statement is poisoned"))?
                .execute_partitions()
                .map_err(adbc_rpc_error)?;
            let schema = protocol::encode_schema(&result.schema).map_err(protocol_rpc_error)?;
            let partitions = serde_json::to_string(&result.partitions)
                .map_err(|error| RpcError::runtime_error(error.to_string()))?;
            Ok(Some(
                RecordBatch::try_new(
                    protocol::partitions_response_schema(),
                    vec![
                        Arc::new(Int64Array::from(vec![result.rows_affected])),
                        Arc::new(BinaryArray::from_vec(vec![schema.as_slice()])),
                        Arc::new(StringArray::from(vec![partitions])),
                    ],
                )
                .map_err(arrow_rpc_error)?,
            ))
        },
    ));

    let close_manager = manager;
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_STATEMENT,
        protocol::statement_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&close_manager, request, ctx)?;
            session
                .close_statement(string(request, "statement_id")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));
}

fn register_result_operations(server: &mut RpcServer, manager: Arc<SessionManager>) {
    let close_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::CLOSE_RESULT,
        protocol::result_schema(),
        protocol::empty_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&close_manager, request, ctx)?;
            session
                .close_result(string(request, "result_id")?)
                .map_err(adbc_rpc_error)?;
            Ok(Some(ok_batch()?))
        },
    ));

    let batch_manager = manager.clone();
    server.register(MethodInfo::unary(
        protocol::method::READ_RESULT_BATCH,
        protocol::read_result_schema(),
        protocol::read_result_batch_response_schema(),
        move |request, ctx| {
            let (session, _) = get_session(&batch_manager, request, ctx)?;
            let result_id = string(request, "result_id")?;
            let sequence =
                protocol::int64_value(&request.batch, "sequence").map_err(protocol_rpc_error)?;
            let result = session.result(result_id).map_err(adbc_rpc_error)?;
            let batch = result
                .lock()
                .map_err(|_| RpcError::runtime_error("result is poisoned"))?
                .next(sequence)
                .map_err(adbc_rpc_error)?;
            let payload = batch
                .as_ref()
                .map(protocol::encode_result_batch)
                .transpose()
                .map_err(protocol_rpc_error)?;
            Ok(Some(
                RecordBatch::try_new(
                    protocol::read_result_batch_response_schema(),
                    vec![
                        Arc::new(BooleanArray::from(vec![batch.is_none()])),
                        Arc::new(BinaryArray::from(vec![payload.as_deref()])),
                    ],
                )
                .map_err(arrow_rpc_error)?,
            ))
        },
    ));

    let handler_manager = manager.clone();
    let decoder_manager = manager;
    let decoder: StateDecoder = Arc::new(move |bytes: &[u8]| {
        let cursor: ResultCursor = serde_json::from_slice(bytes)
            .map_err(|error| RpcError::protocol_error(format!("decode result cursor: {error}")))?;
        Ok(StreamStateKind::Producer(Box::new(ResultProducer {
            manager: decoder_manager.clone(),
            cursor,
        })))
    });
    server.register(
        MethodInfo::stream(
            protocol::method::READ_RESULT,
            MethodType::Producer,
            protocol::read_result_schema(),
            move |request, ctx| {
                let (session, session_id) = get_session(&handler_manager, request, ctx)?;
                let result_id = string(request, "result_id")?.to_string();
                let sequence = protocol::int64_value(&request.batch, "sequence")
                    .map_err(protocol_rpc_error)?;
                let result = session.result(&result_id).map_err(adbc_rpc_error)?;
                let schema = result
                    .lock()
                    .map_err(|_| RpcError::runtime_error("result is poisoned"))?
                    .schema();
                Ok(StreamResult::producer(
                    schema,
                    Box::new(ResultProducer {
                        manager: handler_manager.clone(),
                        cursor: ResultCursor {
                            session_id,
                            result_id,
                            sequence,
                        },
                    }),
                ))
            },
        )
        .with_state_decoder(decoder),
    );
}

fn insert_reader_response(
    session: &crate::session::Session,
    reader: Box<dyn arrow_array::RecordBatchReader + Send + 'static>,
) -> vgi_rpc::Result<RecordBatch> {
    let (result_id, schema) = session.insert_result(reader).map_err(adbc_rpc_error)?;
    protocol::execute_response(&result_id, None, schema.as_ref()).map_err(protocol_rpc_error)
}

fn schema_response(schema: &arrow_schema::Schema) -> vgi_rpc::Result<RecordBatch> {
    let bytes = protocol::encode_schema(schema).map_err(protocol_rpc_error)?;
    RecordBatch::try_new(
        protocol::schema_response_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()]))],
    )
    .map_err(arrow_rpc_error)
}

fn value_response(value: &OptionValue) -> vgi_rpc::Result<RecordBatch> {
    let value = serde_json::to_string(&protocol::WireOptionValue::from(value))
        .map_err(|error| RpcError::runtime_error(error.to_string()))?;
    protocol::one_string(protocol::value_response_schema(), &value).map_err(arrow_rpc_error)
}

fn decode_option_value(value: &str) -> vgi_rpc::Result<OptionValue> {
    serde_json::from_str::<protocol::WireOptionValue>(value)
        .map_err(|error| RpcError::value_error(format!("invalid option value: {error}")))?
        .into_adbc()
        .map_err(protocol_rpc_error)
}

fn json_args<T: serde::de::DeserializeOwned>(request: &Request) -> vgi_rpc::Result<T> {
    serde_json::from_str(string(request, "args_json")?)
        .map_err(|error| RpcError::value_error(format!("invalid method arguments: {error}")))
}

fn get_session(
    manager: &SessionManager,
    request: &Request,
    ctx: &CallContext,
) -> vgi_rpc::Result<(Arc<crate::session::Session>, String)> {
    let principal = manager.principal(&ctx.auth)?;
    let session_id = string(request, "session_id")?.to_string();
    let session = manager
        .get(&session_id, &principal)
        .map_err(adbc_rpc_error)?;
    Ok((session, session_id))
}

fn string<'a>(request: &'a Request, name: &str) -> vgi_rpc::Result<&'a str> {
    protocol::string_value(&request.batch, name).map_err(protocol_rpc_error)
}

fn binary<'a>(request: &'a Request, name: &str) -> vgi_rpc::Result<&'a [u8]> {
    protocol::binary_value(&request.batch, name).map_err(protocol_rpc_error)
}

fn ok_batch() -> vgi_rpc::Result<RecordBatch> {
    RecordBatch::try_new(
        protocol::empty_response_schema(),
        vec![Arc::new(BooleanArray::from(vec![true]))],
    )
    .map_err(arrow_rpc_error)
}

fn adbc_rpc_error(error: AdbcError) -> RpcError {
    let wire = protocol::WireAdbcError::from(&error);
    let encoded = serde_json::to_string(&wire).unwrap_or_else(|_| error.message.clone());
    RpcError::new("AdbcError", encoded)
        .with_error_kind(format!("adbc.{}", protocol::status_name(error.status)))
}

fn protocol_rpc_error(error: protocol::ProtocolError) -> RpcError {
    RpcError::value_error(error.to_string())
}

fn arrow_rpc_error(error: arrow_schema::ArrowError) -> RpcError {
    RpcError::new("ArrowError", error.to_string())
}
