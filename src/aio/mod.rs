//! Native async on the Rust core (`mssql_python.aio`). See docs/ASYNC.md.
//!
//! * `hub`: per-event-loop completion hub (futures, wake-ups, Python conversion).
//! * `session`: the async TDS session state machine (no Python).
//! * `connection` / `pool`: the pyclasses the Python layer drives.
//! * `convert`: decoded rows -> `Row` objects / tuples.

mod connection;
mod convert;
mod hub;
mod pool;
mod session;

use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<hub::Hub>()?;
    m.add_class::<connection::AioConnection>()?;
    m.add_class::<pool::AioPool>()?;
    m.add_class::<convert::RowBatch>()?;
    m.add_function(wrap_pyfunction!(connection::connect, m)?)?;
    m.add_function(wrap_pyfunction!(pool::create_pool, m)?)?;
    Ok(())
}
