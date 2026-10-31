//! Wire ownership: which statement's batch is live on the connection's one
//! TDS stream, and what happens to it when somebody else needs the stream.
//!
//! TDS without MARS allows one active request per connection. When a
//! different statement (or a commit/rollback) needs the connection, the
//! owner's remaining results are moved into the owner's own buffers
//! ([`buffer_owner`]), bounded by [`DRAIN_BUDGET_BYTES`], so both cursors
//! keep working as with msodbcsql + MARS. When the owner itself is going away
//! (closed, freed, re-executed, connection closing) its batch is abandoned
//! instead of read ([`discard_owner`]).

use crate::runtime::Lockable;
use std::sync::{Arc, Weak};

use mssql_tds::connection::tds_client::{ResultSet, StatementResult};
use mssql_tds::error::Error as TdsError;

use super::StmtShared;
use super::cancel::StmtCancel;
use super::cursor::{BufferedResult, StmtState, fill_pending, read_rows};
use crate::decode::RowBuffer;
use crate::diag::{self, DiagRecord};
use crate::runtime::SyncClient;

/// The TDS client plus which statement currently owns its live batch.
pub struct Slot {
    pub client: Option<SyncClient>,
    pub owner: Option<(u64, Weak<StmtShared>)>,
}

/// Most bytes buffered for one statement when its results are moved off the
/// wire. Beyond it the rest of the batch is abandoned and the owner's cursor
/// reports an error once it reaches the cut.
pub const DRAIN_BUDGET_BYTES: usize = 64 << 20;
/// How long to wait for the server to acknowledge an abandoned request.
const ABANDON_SETTLE_SECS: u32 = 5;
/// Rows of an abandoned result that are read and dropped before giving up and
/// sending an attention instead.
const SKIM_BUDGET_BYTES: usize = 8 << 20;
const SKIM_CHUNK_ROWS: usize = 4096;

/// Free the wire for statement `requester`. Another statement's results are
/// buffered; the requester's own previous batch is abandoned. The buffering
/// runs under the requester's timeout and cancellation.
pub fn free_for_statement(
    slot: &mut Slot,
    requester: &StmtShared,
    cancel: &Arc<StmtCancel>,
    timeout_secs: u32,
) {
    let Slot { client, owner } = slot;
    let Some(client) = client.as_mut() else {
        *owner = None;
        return;
    };
    if let Some((id, weak)) = owner.take() {
        match weak.upgrade() {
            Some(owned) if id == requester.id => discard_owner(client, &owned),
            Some(owned) => buffer_owner(client, &owned, Some((cancel, timeout_secs))),
            // The owner is gone and nobody knows how much of its batch is left.
            None => abandon(client, false),
        }
    }
    abandon(client, !cancel.is_interrupted());
}

/// Free the wire for a transaction or session request of the connection.
pub fn free_for_connection(slot: &mut Slot) {
    let Slot { client, owner } = slot;
    let Some(client) = client.as_mut() else {
        *owner = None;
        return;
    };
    if let Some((_, weak)) = owner.take() {
        match weak.upgrade() {
            Some(owned) => buffer_owner(client, &owned, None),
            None => abandon(client, false),
        }
    }
    abandon(client, true);
}

/// Abandon whatever batch is live (connection close).
pub fn discard_all(slot: &mut Slot) {
    let Slot { client, owner } = slot;
    let Some(client) = client.as_mut() else {
        *owner = None;
        return;
    };
    if let Some((_, weak)) = owner.take() {
        match weak.upgrade() {
            Some(owned) => discard_owner(client, &owned),
            None => abandon(client, false),
        }
    }
    abandon(client, true);
}

/// Abandon the batch of `slot.owner` if it belongs to `statement_id`.
pub fn discard_if_owner(slot: &mut Slot, statement_id: u64) {
    if slot.owner.as_ref().is_some_and(|(id, _)| *id == statement_id) {
        discard_all(slot);
    }
}

/// Discard the current result of `stmt` if it is still generation
/// `generation`: an Arrow stream was dropped before its end, so nobody will
/// read the rest.
pub fn abandon_result(stmt: &StmtShared, generation: u64) {
    let mut slot = stmt.conn.slot.lock_checked();
    if stmt.state.lock_checked().generation != generation {
        return;
    }
    discard_if_owner(&mut slot, stmt.id);
    let mut state = stmt.state.lock_checked();
    if state.generation == generation {
        state.reset_results();
    }
}

fn discard_owner(client: &mut SyncClient, owned: &StmtShared) {
    let mut state = owned.state.lock_checked();
    // A read error also ends the result: only `close_query` resynchronises
    // after one.
    let exhausted = state.result_done;
    state.on_wire = false;
    state.result_done = true;
    abandon(client, exhausted);
}

/// End the request on the wire. A result that is nearly over is skimmed to its
/// end and the batch closed normally; only a result with a lot of data still
/// to come is cut short with a TDS attention, which is what makes closing a
/// cursor over a huge result cheap.
fn abandon(client: &mut SyncClient, exhausted: bool) {
    if !client.has_open_batch() {
        return;
    }
    let _ = if exhausted || skim_to_end(client) {
        client.block_on(|c| c.close_query())
    } else {
        client.block_on(|c| c.cancel_query(ABANDON_SETTLE_SECS))
    };
    let _ = client.take_info_messages();
}

/// Read and drop the rest of the current result's rows, up to
/// [`SKIM_BUDGET_BYTES`]. Returns whether the result ended within the budget.
fn skim_to_end(client: &mut SyncClient) -> bool {
    if !client.on_rows() {
        return true;
    }
    let mut scratch = RowBuffer::new();
    let mut skimmed = 0usize;
    loop {
        match read_rows(client, &mut scratch, SKIM_CHUNK_ROWS, usize::MAX) {
            Ok(true) => return true,
            Ok(false) => {
                skimmed += scratch.byte_size();
                scratch.clear();
                if skimmed > SKIM_BUDGET_BYTES {
                    return false;
                }
            }
            // The close that follows reports or recovers from the failure.
            Err(_) => return true,
        }
    }
}

/// Move everything `owned` still has on the wire into its own buffers.
/// With a requester, the buffering stops when the requester is cancelled or
/// its timeout expires.
fn buffer_owner(
    client: &mut SyncClient,
    owned: &StmtShared,
    requester: Option<(&Arc<StmtCancel>, u32)>,
) {
    let mut state = owned.state.lock_checked();
    if !state.on_wire {
        return;
    }
    let _deadline = requester.map(|(cancel, timeout_secs)| {
        if let Some(owner_cancel) = owned.cancel.lock().as_ref() {
            cancel.forward_to(owner_cancel.shared_handle());
        }
        cancel.arm_deadline(timeout_secs)
    });
    buffer_all(client, &mut state, DRAIN_BUDGET_BYTES);
    if let Some((cancel, _)) = requester {
        cancel.stop_forwarding();
    }
}

/// Read the rest of the current result and every later result of the batch
/// into `state`, until `budget` bytes are buffered.
pub(super) fn buffer_all(client: &mut SyncClient, state: &mut StmtState, budget: usize) {
    fill_pending(client, state, usize::MAX, budget);
    let over_budget = !state.result_done && state.pending_error.is_none();
    if over_budget {
        cut_off(client, state);
        return;
    }
    let mut used = state.pending.byte_size();
    while client.has_open_batch() && !client.is_connection_dead() {
        let advanced = client.block_on(|c| c.advance());
        let info: Vec<DiagRecord> = diag::info_records(&client.take_info_messages()).collect();
        match advanced {
            Ok(StatementResult::End) => {
                let errors = super::cursor::prepared_batch_errors(client);
                if !errors.is_empty() {
                    state.queued.push_back(BufferedResult::failed(errors, info));
                }
                break;
            }
            Ok(StatementResult::NoRows { rows_affected }) => state
                .queued
                .push_back(BufferedResult::without_rows(rows_affected.map_or(-1, |n| n as i64), info)),
            Ok(StatementResult::Rows) => {
                let columns = Some(Arc::new(client.get_metadata().clone()));
                let mut rows = RowBuffer::new();
                let outcome = read_rows(client, &mut rows, usize::MAX, budget.saturating_sub(used));
                used += rows.byte_size();
                let (error, batch_goes_on, read_failed) = match outcome {
                    Ok(true) => (None, true, false),
                    Ok(false) => (Some(vec![over_budget_record()]), false, false),
                    Err(e) => (
                        Some(diag::records_for_error(&e, "HY000")),
                        batch_continues(client, Some(&e)),
                        true,
                    ),
                };
                state.queued.push_back(BufferedResult { columns, rows, rowcount: -1, info, error });
                if !batch_goes_on {
                    // Only `close_query` resynchronises after a read error.
                    abandon(client, read_failed);
                    break;
                }
            }
            Err(err) => {
                state
                    .queued
                    .push_back(BufferedResult::failed(diag::records_for_error(&err, "HY000"), info));
                if !batch_continues(client, Some(&err)) {
                    break;
                }
            }
        }
    }
    state.on_wire = false;
}

/// After a read error: does the batch go on (a statement error under
/// `BatchErrorMode::Continue`) or is it over?
fn batch_continues(client: &SyncClient, err: Option<&TdsError>) -> bool {
    client.has_open_batch()
        && !client.is_connection_dead()
        && err.is_none_or(|e| matches!(e, TdsError::SqlServerError { .. }))
}

/// The budget ran out inside the current result: the rest is abandoned and
/// the cursor reports it once the buffered rows are delivered.
fn cut_off(client: &mut SyncClient, state: &mut StmtState) {
    state.result_done = true;
    state.pending_error = Some(vec![over_budget_record()]);
    state.on_wire = false;
    abandon(client, false);
}

fn over_budget_record() -> DiagRecord {
    DiagRecord::driver(
        "HY000",
        format!(
            "Result set discarded: more than {} MiB were pending when another statement needed the connection",
            DRAIN_BUDGET_BYTES >> 20
        ),
    )
}
