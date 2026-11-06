//! Row decoding without Python:
//! - `buffer`: `RowBuffer`, the `RowWriter` sink row fetches decode into;
//! - `describe`: ODBC metadata (SQLDescribeCol) for a result column;
//! - `hidden`: dropping server-hidden columns (browse-mode keys);
//! - `ascii`: narrowing pure-ASCII UTF-16 text.

pub mod ascii;
pub mod buffer;
pub mod describe;
pub mod hidden;

pub use buffer::RowBuffer;
