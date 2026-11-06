//! SQL Server to Arrow over native TDS, without Python.
//!
//! The Python package `arrowsql` is a thin pyo3 layer over this crate. Rust
//! code uses the facade at the crate root:
//!
//! ```no_run
//! use arrowsql_core::{ConnectOptions, Connection};
//!
//! let conn = Connection::connect(&ConnectOptions::new("Server=localhost;Uid=sa;Pwd=...;TrustServerCertificate=yes"))?;
//! let mut stmt = conn.execute("SELECT name FROM sys.databases", &[])?;
//! while let Some(batch) = stmt.next_batch(65_536)? {
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok::<(), arrowsql_core::Error>(())
//! ```
//!
//! [`read_arrow_stream`] splits a read on a column across connections and
//! merges the batches into one [`ParallelReader`]. To hand the partitions to
//! separate consumers instead (Spark tasks, pyarrow in another library),
//! [`plan_partitions`] returns the [`Slice`]s and each one opens as its own
//! reader or Arrow C stream ([`Slice::open_ffi_stream`]).
//!
//! The modules below the facade are the driver internals the Python bindings
//! build on (ODBC-style statement handles, diagnostics, pooling). They are
//! public for that crate only and carry no stability promise.
//!
//! Hooks a host process installs: [`runtime::set_interrupt_check`] (Python's
//! signal check), the `log` facade for messages, and [`TokenProvider`] /
//! [`FedAuthProvider`] for Entra ID tokens. This crate declares no global
//! allocator.

mod api;
mod error;
pub mod read;
pub mod utf8;

pub use api::{
    CancelHandle, ConnectOptions, Connection, FedAuthProvider, Param, Statement, StatementReader,
    TokenProvider,
};
pub use error::{Error, ErrorKind};
pub use read::{
    Connector, ParallelReader, Partitioning, ReadOptions, Slice, plan_partitions, read_arrow,
    read_arrow_stream, read_arrow_stream_with,
};

pub use arrow_array::ffi_stream::FFI_ArrowArrayStream;
pub use arrow_array::{RecordBatch, RecordBatchReader};
pub use arrow_schema::{ArrowError, SchemaRef};

#[doc(hidden)]
pub mod arrow;
#[doc(hidden)]
pub mod connection;
#[doc(hidden)]
pub mod connstr;
#[doc(hidden)]
pub mod consts;
#[doc(hidden)]
pub mod decode;
#[doc(hidden)]
pub mod diag;
#[doc(hidden)]
pub mod fmt;
#[doc(hidden)]
pub mod numeric;
#[doc(hidden)]
pub mod params;
#[doc(hidden)]
pub mod pooling;
pub mod runtime;
#[doc(hidden)]
pub mod statement;
#[doc(hidden)]
pub mod txn_abort;
#[doc(hidden)]
pub mod utf8_wrap;

/// Forget process-wide state inherited across `fork(2)`: idle pooled sessions
/// and the shared runtime belong to the parent. Call in the child, before any
/// other use of this crate.
pub fn reset_after_fork() {
    pooling::forget_after_fork();
    runtime::forget_after_fork();
}
