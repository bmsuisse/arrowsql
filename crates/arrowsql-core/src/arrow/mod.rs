//! Native Arrow fetch.
//!
//! * `plan`: SQL column -> Arrow type (identical to upstream's C++ binding).
//! * `sink`: `ArrowSink`, a `RowWriter` decoding TDS rows straight into Arrow
//!   buffers; plugged into `statement::fetch_into` like `RowBuffer` is into
//!   the row fetch.
//! * `stream`: batches decoded ahead on a producer thread, and the merge of
//!   several such streams.

pub mod plan;
pub mod sink;
pub mod stream;

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::consts::*;
use crate::diag::DiagRecord;
use crate::statement::{self, BatchSink, StmtCancel, StmtShared};
use plan::Plan;
use sink::ArrowSink;

/// What [`fetch_table`] read.
pub struct FetchedTable {
    /// `SQL_SUCCESS`, `SQL_SUCCESS_WITH_INFO`, or the failing code (the
    /// statement's diagnostics say why); `Err` when a batch could not be built.
    pub ret: Result<SqlReturn, String>,
    /// Info records the fetches produced.
    pub info: Vec<DiagRecord>,
    /// The batches delivered (before the error, if any).
    pub batches: Vec<RecordBatch>,
}

/// Fetch up to `max_rows` (`usize::MAX`: all) remaining rows of the current
/// result as batches of `batch_size` rows, on the calling thread.
pub fn fetch_table(stmt: &StmtShared, plan: &Plan, mut max_rows: usize, batch_size: usize) -> FetchedTable {
    let batch_size = batch_size.max(1);
    let mut batches = Vec::new();
    let mut info = Vec::new();
    let mut ret = SQL_SUCCESS;
    while max_rows > 0 {
        let want = max_rows.min(batch_size);
        let mut sink = ArrowSink::new(plan, want);
        let fetched = statement::fetch_into(stmt, &mut sink, want, None);
        info.extend(fetched.info);
        match fetched.ret {
            SQL_SUCCESS => {}
            SQL_SUCCESS_WITH_INFO => ret = SQL_SUCCESS_WITH_INFO,
            SQL_NO_DATA => break,
            other => {
                ret = other;
                break;
            }
        }
        let got = sink.rows();
        match sink.finish() {
            Ok(batch) => batches.push(batch),
            Err(err) => return FetchedTable { ret: Err(err), info, batches },
        }
        max_rows -= got;
        // A short batch is not the end: an error that cut it short is
        // reported by the next fetch, which is what ends the loop.
    }
    FetchedTable { ret: Ok(ret), info, batches }
}

/// Rows per batch when the caller doesn't care (native API default).
pub const DEFAULT_BATCH_ROWS: usize = 65_536;

/// The Arrow plan of the statement's current result, its generation and the
/// cancel of the execution that produced it; `Err` without a result set.
pub fn current_plan(stmt: &StmtShared) -> Result<(Plan, u64, Option<Arc<StmtCancel>>), String> {
    let Some((columns, generation, execution)) = statement::result_columns(stmt) else {
        return Err("No active result set. Cannot fetch Arrow batch.".to_string());
    };
    let plan = Plan::new(&columns)?;
    Ok((plan, generation, execution))
}
