//! Arrow record batches of a result, decoded ahead on a producer thread.
//!
//! * [`StreamCore`]: rows still arriving. A producer thread decodes batch N+1
//!   while the consumer works on batch N (bounded queue). Taking a reader
//!   without reading a batch (a schema peek) leaves it available for the next
//!   reader.
//! * [`MergeCore`]: the batches of several `StreamCore`s (one per connection)
//!   as one unordered stream with a bounded buffer; the partitioned read feeds
//!   one.
//!
//! Neither waits for an interpreter lock; a host that holds one (Python's GIL)
//! releases it around calls that block and around dropping the last handle.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

use super::plan::Plan;
use super::sink::ArrowSink;
use crate::consts::*;
use crate::statement::{self, BatchSink, StmtCancel, StmtShared};

/// A decoded batch, or the error text (`[SQLSTATE] message; ...`) that ended
/// the stream.
pub type BatchResult = Result<RecordBatch, String>;

/// Why a reader could not be taken from a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderError {
    /// `shutdown` was called.
    Closed,
    /// A reader already handed out batches.
    Consumed,
    /// Another reader is open.
    Busy,
}

/// Shared by a statement's stream handle and every reader taken from it.
pub struct StreamCore {
    schema: SchemaRef,
    stmt: Arc<StmtShared>,
    generation: u64,
    /// Cancellation of the execution this stream reads: closing the stream
    /// interrupts that execution only. The statement's current execution may
    /// be a newer query on the same cursor.
    execution: Option<Arc<StmtCancel>>,
    rx: Mutex<Option<Receiver<BatchResult>>>,
    /// `shutdown()` was called: readers end, `rx` may still be held by one.
    closing: AtomicBool,
    producer: Mutex<Option<JoinHandle<()>>>,
    /// A reader exists that hasn't been dropped yet.
    exporting: AtomicBool,
    /// A batch was handed out: no other reader can be taken.
    consumed: AtomicBool,
    /// The producer reached the end of the result.
    exhausted: Arc<AtomicBool>,
    /// Kept alive for as long as the stream (the Python bindings put the
    /// cursor here: freeing it would free the statement mid-stream).
    _keep_alive: Option<Box<dyn Any + Send + Sync>>,
}

impl StreamCore {
    /// Start decoding the current result of `stmt` (`generation`, from
    /// [`statement::result_columns`]) on a producer thread, `depth` batches
    /// of `batch_rows` rows ahead.
    pub fn start(
        stmt: Arc<StmtShared>,
        plan: Plan,
        generation: u64,
        execution: Option<Arc<StmtCancel>>,
        batch_rows: usize,
        depth: usize,
        keep_alive: Option<Box<dyn Any + Send + Sync>>,
    ) -> Arc<StreamCore> {
        let (tx, rx) = sync_channel(depth.max(1));
        let exhausted = Arc::new(AtomicBool::new(false));
        let producer = {
            let (stmt, plan, exhausted) = (stmt.clone(), plan.clone(), exhausted.clone());
            std::thread::Builder::new()
                .name("mssql-rs-arrow".into())
                .spawn(move || produce(stmt, plan, generation, batch_rows, tx, exhausted))
                .expect("failed to spawn Arrow producer thread")
        };
        Arc::new(StreamCore {
            schema: plan.schema,
            stmt,
            generation,
            execution,
            rx: Mutex::new(Some(rx)),
            closing: AtomicBool::new(false),
            producer: Mutex::new(Some(producer)),
            exporting: AtomicBool::new(false),
            consumed: AtomicBool::new(false),
            exhausted,
            _keep_alive: keep_alive,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn is_closed(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// The one reader of this stream. A reader dropped before it handed out
    /// a batch leaves the stream available for the next one.
    pub fn reader(self: &Arc<Self>) -> Result<StreamReader, ReaderError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(ReaderError::Closed);
        }
        if self.consumed.load(Ordering::Acquire) {
            return Err(ReaderError::Consumed);
        }
        if self.exporting.swap(true, Ordering::AcqRel) {
            return Err(ReaderError::Busy);
        }
        Ok(StreamReader {
            core: self.clone(),
            pulled: false,
        })
    }

    /// Cancel the execution this stream reads (a statement still running
    /// stops; the stream then ends with the cancellation error).
    pub fn cancel(&self) {
        if let Some(execution) = &self.execution {
            execution.cancel();
        }
    }

    /// Stop the producer and give the connection back: unread rows of the
    /// result are discarded unless it was read to the end.
    ///
    /// Never waits for a reader: one parked in `recv` holds the `rx` lock, so
    /// the producer is interrupted first and the reader wakes once it is gone.
    pub fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        if let Some(handle) = self.producer.lock().unwrap().take() {
            let mut backoff = Duration::from_micros(50);
            while !handle.is_finished() {
                // A producer blocked in `send` wakes when the receiver goes;
                // one blocked inside a read (e.g. on a row lock) never sees the
                // closed channel: interrupt its read until it is gone.
                if let Ok(mut rx) = self.rx.try_lock() {
                    drop(rx.take());
                }
                self.cancel();
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(5));
            }
            let _ = handle.join();
            if !self.exhausted.load(Ordering::Acquire) {
                statement::abandon_result(&self.stmt, self.generation);
            }
        }
        // The producer is gone, so a reader in `recv` has returned.
        drop(self.rx.lock().unwrap().take());
    }
}

impl Drop for StreamCore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn produce(
    stmt: Arc<StmtShared>,
    plan: Plan,
    generation: u64,
    batch_rows: usize,
    tx: SyncSender<BatchResult>,
    exhausted: Arc<AtomicBool>,
) {
    loop {
        let mut sink = ArrowSink::new(&plan, batch_rows);
        let fetched = statement::fetch_into(&stmt, &mut sink, batch_rows, Some(generation));
        if fetched.stale {
            return;
        }
        if fetched.ret == SQL_ERROR {
            let message = statement::diag_records(&stmt)
                .iter()
                .map(|r| format!("[{}] {}", r.sqlstate, r.message))
                .collect::<Vec<_>>()
                .join("; ");
            exhausted.store(true, Ordering::Release);
            let _ = tx.send(Err(message));
            return;
        }
        if sink.rows() == 0 {
            exhausted.store(true, Ordering::Release);
            return;
        }
        if tx.send(sink.finish()).is_err() {
            return;
        }
    }
}

/// The reader of a [`StreamCore`]. Dropping it after it handed out a batch
/// shuts the stream down.
pub struct StreamReader {
    core: Arc<StreamCore>,
    pulled: bool,
}

impl StreamReader {
    pub fn core(&self) -> &Arc<StreamCore> {
        &self.core
    }
}

impl Iterator for StreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pulled = true;
        self.core.consumed.store(true, Ordering::Release);
        if self.core.closing.load(Ordering::Acquire) {
            return None;
        }
        let rx = self.core.rx.lock().unwrap();
        match rx.as_ref()?.recv() {
            _ if self.core.closing.load(Ordering::Acquire) => None,
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(message)) => Some(Err(ArrowError::ExternalError(message.into()))),
            Err(_) => None, // producer finished
        }
    }
}

impl RecordBatchReader for StreamReader {
    fn schema(&self) -> SchemaRef {
        self.core.schema.clone()
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        self.core.exporting.store(false, Ordering::Release);
        if self.pulled {
            self.core.shutdown();
        }
    }
}

/// Shared by the merged stream's handle, the workers that feed it and any
/// reader taken from it.
pub struct MergeCore {
    schema: SchemaRef,
    /// Master sender, cloned per `pump`; dropped when the last worker is done,
    /// which ends the merged stream.
    tx: Mutex<Option<SyncSender<BatchResult>>>,
    workers: AtomicUsize,
    rx: Mutex<Option<Receiver<BatchResult>>>,
    /// The consumer side is finished or closed.
    closing: AtomicBool,
    /// Workers must stop (closed, or a worker failed).
    stop: AtomicBool,
    /// Streams being pumped right now, so a stop can interrupt their reads.
    active: Mutex<Vec<Arc<StreamCore>>>,
    exporting: AtomicBool,
    consumed: AtomicBool,
}

impl MergeCore {
    /// `workers`: how many feeders will call [`worker_done`](Self::worker_done).
    /// `depth`: batches buffered between the workers and the consumer.
    pub fn new(schema: SchemaRef, workers: usize, depth: usize) -> Arc<MergeCore> {
        let (tx, rx) = sync_channel(depth.max(1));
        Arc::new(MergeCore {
            schema,
            tx: Mutex::new(if workers == 0 { None } else { Some(tx) }),
            workers: AtomicUsize::new(workers),
            rx: Mutex::new(Some(rx)),
            closing: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            active: Mutex::new(Vec::new()),
            exporting: AtomicBool::new(false),
            consumed: AtomicBool::new(false),
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Interrupt every stream being pumped (workers then see `stop`).
    fn cancel_active(&self) {
        let active: Vec<Arc<StreamCore>> = self.active.lock().unwrap().clone();
        for core in active {
            core.shutdown();
        }
    }

    /// End the merged stream: discard buffered batches and stop the workers.
    /// Never waits for a reader parked in `recv` (it wakes when the workers are
    /// done and the last sender is gone).
    pub fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        self.cancel_active();
        if let Ok(mut rx) = self.rx.try_lock() {
            drop(rx.take());
        }
    }

    /// Report a worker failure to the consumer and stop the other workers.
    pub fn fail(&self, message: String) {
        self.stop.store(true, Ordering::Release);
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(Err(message));
        }
        self.cancel_active();
    }

    /// Move every batch of `source` into the merge (blocks until the stream
    /// ends, the merge stops or a read fails; a failure is delivered to the
    /// consumer). The source is shut down on return.
    pub fn pump(&self, source: &Arc<StreamCore>) -> Result<(), ReaderError> {
        let mut reader = source.reader()?;
        self.active.lock().unwrap().push(source.clone());
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            while !self.stop.load(Ordering::Acquire) {
                match reader.next() {
                    None => break,
                    Some(Ok(batch)) => {
                        let batch = if batch.schema() == self.schema {
                            batch
                        } else {
                            batch
                                .clone()
                                .with_schema(self.schema.clone())
                                .unwrap_or(batch)
                        };
                        if tx.send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        self.fail(err.to_string());
                        break;
                    }
                }
            }
        }
        drop(reader);
        self.active
            .lock()
            .unwrap()
            .retain(|c| !Arc::ptr_eq(c, source));
        Ok(())
    }

    /// A worker is finished; the last one ends the stream.
    pub fn worker_done(&self) {
        if self.workers.fetch_sub(1, Ordering::AcqRel) == 1 {
            drop(self.tx.lock().unwrap().take());
        }
    }

    /// Workers should stop picking up work (closed or failed).
    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    pub fn closed(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// The stream was handed to a consumer (reader taken or batches read).
    pub fn in_use(&self) -> bool {
        self.exporting.load(Ordering::Acquire) || self.consumed.load(Ordering::Acquire)
    }

    /// Reading by [`recv_timeout`](Self::recv_timeout) counts as consuming.
    pub fn mark_consumed(&self) {
        self.consumed.store(true, Ordering::Release);
    }

    /// The next batch, waiting at most `timeout` (for a host that looks at
    /// its own interruptions in between). `Disconnected` ends the stream.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<BatchResult, RecvTimeoutError> {
        let rx = self.rx.lock().unwrap();
        match rx.as_ref() {
            Some(rx) => rx.recv_timeout(timeout),
            None => Err(RecvTimeoutError::Disconnected),
        }
    }

    /// The one reader of the merged stream.
    pub fn reader(self: &Arc<Self>) -> Result<MergeReader, ReaderError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(ReaderError::Closed);
        }
        if self.consumed.load(Ordering::Acquire) {
            return Err(ReaderError::Consumed);
        }
        if self.exporting.swap(true, Ordering::AcqRel) {
            return Err(ReaderError::Busy);
        }
        Ok(MergeReader {
            core: self.clone(),
            pulled: false,
        })
    }
}

/// The reader of a [`MergeCore`]. Dropping it after it handed out a batch
/// stops the workers.
pub struct MergeReader {
    core: Arc<MergeCore>,
    pulled: bool,
}

impl Iterator for MergeReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pulled = true;
        self.core.consumed.store(true, Ordering::Release);
        if self.core.closing.load(Ordering::Acquire) {
            return None;
        }
        let item = {
            let rx = self.core.rx.lock().unwrap();
            rx.as_ref()?.recv()
        };
        match item {
            _ if self.core.closing.load(Ordering::Acquire) => None,
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(message)) => {
                self.core.shutdown();
                Some(Err(ArrowError::ExternalError(message.into())))
            }
            Err(_) => None, // every worker is done
        }
    }
}

impl RecordBatchReader for MergeReader {
    fn schema(&self) -> SchemaRef {
        self.core.schema.clone()
    }
}

impl Drop for MergeReader {
    fn drop(&mut self) {
        self.core.exporting.store(false, Ordering::Release);
        if self.pulled {
            self.core.shutdown();
        }
    }
}
