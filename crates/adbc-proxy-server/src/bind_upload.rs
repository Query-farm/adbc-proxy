use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;

use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;

type Reader = Box<dyn RecordBatchReader + Send + 'static>;
type Reply<T> = mpsc::Sender<Result<T, AdbcError>>;

enum Command {
    Push(RecordBatch, Reply<()>),
    Finish(Reply<Reader>),
    Cancel,
}

/// A bounded, per-upload actor. VGI delivers native batches one turn at a
/// time; this actor writes them to an anonymous file and acknowledges only
/// after the write completed. That bounds heap use while producing the owned
/// reader required by ADBC implementations that retain `bind_stream` input.
pub struct BindUpload {
    tx: SyncSender<Command>,
}

impl BindUpload {
    pub fn start(schema: SchemaRef, max_bytes: usize) -> Result<Self, AdbcError> {
        let (tx, rx) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("adbc-proxy-bind-upload".to_string())
            .spawn(move || run(rx, schema, max_bytes))
            .map_err(|error| io_error(format!("start bind upload worker: {error}")))?;
        Ok(Self { tx })
    }

    pub fn push(&self, batch: RecordBatch) -> Result<(), AdbcError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(Command::Push(batch, reply_tx))
            .map_err(|_| invalid_state("bind upload worker stopped"))?;
        reply_rx
            .recv()
            .map_err(|_| invalid_state("bind upload worker stopped"))?
    }

    pub fn finish(self) -> Result<Reader, AdbcError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(Command::Finish(reply_tx))
            .map_err(|_| invalid_state("bind upload worker stopped"))?;
        reply_rx
            .recv()
            .map_err(|_| invalid_state("bind upload worker stopped"))?
    }

    pub fn cancel(self) {
        let _ = self.tx.try_send(Command::Cancel);
    }
}

struct CountingFile {
    file: std::fs::File,
    written: usize,
}

impl Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let count = self.file.write(buf)?;
        self.written = self.written.saturating_add(count);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Read for CountingFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for CountingFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

fn run(rx: Receiver<Command>, schema: SchemaRef, max_bytes: usize) {
    let file = match tempfile::tempfile() {
        Ok(file) => file,
        Err(error) => {
            fail_all(rx, io_error(format!("create bind upload file: {error}")));
            return;
        }
    };
    let counting = CountingFile { file, written: 0 };
    let mut writer = match StreamWriter::try_new(counting, schema.as_ref()) {
        Ok(writer) => writer,
        Err(error) => {
            fail_all(rx, invalid_data(format!("initialize bind upload: {error}")));
            return;
        }
    };
    let mut batches = 0usize;
    let mut terminal: Option<AdbcError> = None;

    while let Ok(command) = rx.recv() {
        match command {
            Command::Push(batch, reply) => {
                let result = if let Some(error) = &terminal {
                    Err(error.clone())
                } else {
                    writer
                        .write(&batch)
                        .map_err(|error| invalid_data(format!("stage bind batch: {error}")))
                        .and_then(|()| {
                            let actual = writer.get_ref().written;
                            if actual > max_bytes {
                                Err(invalid_data(format!(
                                    "bind stream exceeds the {max_bytes} byte limit ({actual} bytes)"
                                )))
                            } else {
                                batches += 1;
                                Ok(())
                            }
                        })
                };
                if let Err(error) = &result {
                    terminal = Some(error.clone());
                }
                let _ = reply.send(result);
            }
            Command::Finish(reply) => {
                let result = if let Some(error) = terminal {
                    Err(error)
                } else {
                    finish_reader(writer, max_bytes)
                };
                let _ = reply.send(result);
                break;
            }
            Command::Cancel => break,
        }
    }
    let _ = batches;
}

fn finish_reader(
    writer: StreamWriter<CountingFile>,
    max_bytes: usize,
) -> Result<Reader, AdbcError> {
    let mut file = writer
        .into_inner()
        .map_err(|error| invalid_data(format!("finish bind upload: {error}")))?;
    if file.written > max_bytes {
        return Err(invalid_data(format!(
            "bind stream exceeds the {max_bytes} byte limit ({} bytes)",
            file.written
        )));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(format!("rewind bind upload: {error}")))?;
    let reader = StreamReader::try_new(file, None)
        .map_err(|error| invalid_data(format!("open staged bind stream: {error}")))?;
    Ok(Box::new(reader))
}

fn fail_all(rx: Receiver<Command>, error: AdbcError) {
    while let Ok(command) = rx.recv() {
        match command {
            Command::Push(_, reply) => {
                let _ = reply.send(Err(error.clone()));
            }
            Command::Finish(reply) => {
                let _ = reply.send(Err(error.clone()));
                break;
            }
            Command::Cancel => break,
        }
    }
}

fn invalid_data(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::InvalidData)
}

fn invalid_state(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::InvalidState)
}

fn io_error(message: impl Into<String>) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::IO)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BinaryArray, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn stages_multiple_batches_into_an_owned_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let upload = BindUpload::start(Arc::clone(&schema), 1024 * 1024).unwrap();
        upload
            .push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![1, 2]))],
                )
                .unwrap(),
            )
            .unwrap();
        upload
            .push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![3]))],
                )
                .unwrap(),
            )
            .unwrap();
        let reader = upload.finish().unwrap();
        assert_eq!(
            reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
            3
        );
    }

    #[test]
    fn rejects_a_staged_stream_over_its_cumulative_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let upload = BindUpload::start(Arc::clone(&schema), 512).unwrap();
        let payload = vec![0_u8; 4096];
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_vec(vec![payload.as_slice()]))],
        )
        .unwrap();
        assert_eq!(upload.push(batch).unwrap_err().status, Status::InvalidData);
        assert_eq!(
            upload
                .finish()
                .err()
                .expect("finish must retain error")
                .status,
            Status::InvalidData
        );
    }
}
