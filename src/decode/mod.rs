//! Row decoding. One file per SQL type family, plus:
//! - `buffer`: `RowBuffer`, the GIL-free `RowWriter` sink fetches decode into;
//! - `python`: turns a filled `RowBuffer` into Python lists;
//! - `describe`: ODBC metadata (SQLDescribeCol) for a result column.
//!
//! To add a type: handle its `RowWriter::write_*` call in `buffer.rs` (add a
//! `Cell` variant if needed), convert it in `python.rs` via a family module,
//! and map its metadata in `describe.rs`.

pub mod binary;
pub mod buffer;
pub mod describe;
pub mod hidden;
pub mod numeric;
pub mod python;
pub mod temporal;
pub mod text;

pub use buffer::RowBuffer;
