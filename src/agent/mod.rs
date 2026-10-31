//! Agent mode: bounded, typed text previews of query results.
//!
//! `AgentRender` collects the parts of one statement's output (result sets,
//! rows affected, server messages) under a single byte ceiling. A result set
//! is fetched in small Arrow chunks straight from the statement (`ArrowSink`,
//! GIL released) and formatted by `render::Table` as it arrives; fetching
//! stops as soon as the row or byte limit is reached, so a capped preview of
//! a huge result never decodes or formats more than it shows. The Python side
//! (`mssql_python.agent`) drives execute/nextset and turns errors into JSON.

pub mod fmt;
mod render;
mod sqltype;

use std::time::{Duration, Instant};

use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;

use crate::arrow::plan::Plan;
use crate::arrow::sink::ArrowSink;
use crate::consts::*;
use crate::statement;
use render::{ColumnInfo, Format, Limits, Table};

/// Rows decoded per fetch while the head is being filled: the first chunk is
/// small so a tight byte budget decodes few surplus rows, then it doubles.
const FIRST_CHUNK_ROWS: usize = 32;
const MAX_CHUNK_ROWS: usize = 256;
/// Smallest byte budget worth starting another result set with.
const MIN_TABLE_BYTES: usize = 256;
/// Kept free after the first part for the "N parts omitted" note.
const NOTE_RESERVE: usize = 96;
/// Longest server message shown, in characters.
const MESSAGE_CHARS: usize = 400;

/// Collects the output of one statement under a byte ceiling.
#[pyclass(name = "AgentRender", module = "arrowsql.ddbc_bindings")]
pub struct AgentRender {
    format: Format,
    max_bytes: usize,
    max_rows: usize,
    max_cell_chars: usize,
    tail_rows: usize,
    deadline: Option<Instant>,
    parts: Vec<String>,
    used: usize,
    tables: usize,
    omitted: usize,
}

impl AgentRender {
    /// Bytes a new part may use, after the separator and the omitted-note reserve.
    fn room(&self) -> usize {
        let note = if self.parts.is_empty() { 0 } else { NOTE_RESERVE };
        self.max_bytes.saturating_sub(self.used + usize::from(!self.parts.is_empty()) + note)
    }

    fn push(&mut self, text: String) {
        self.used += text.len() + usize::from(!self.parts.is_empty());
        self.parts.push(text);
    }

    fn push_line(&mut self, line: String) {
        if line.len() <= self.room() {
            self.push(line);
        } else {
            self.omitted += 1;
        }
    }
}

#[pymethods]
impl AgentRender {
    #[new]
    #[pyo3(signature = (format="tsv", max_bytes=10_000, max_rows=1000, max_cell_chars=500, tail_rows=0, timeout_s=None))]
    fn new(
        format: &str,
        max_bytes: i64,
        max_rows: i64,
        max_cell_chars: i64,
        tail_rows: i64,
        timeout_s: Option<f64>,
    ) -> PyResult<AgentRender> {
        let format = Format::parse(format)
            .ok_or_else(|| PyValueError::new_err("format must be 'tsv' or 'markdown'"))?;
        if !(256..=(isize::MAX as i64)).contains(&max_bytes) {
            return Err(PyValueError::new_err("max_bytes must be at least 256"));
        }
        if !(1..(isize::MAX as i64)).contains(&max_rows) {
            return Err(PyValueError::new_err("max_rows must be positive"));
        }
        if !(1..=(isize::MAX as i64)).contains(&max_cell_chars) {
            return Err(PyValueError::new_err("max_cell_chars must be positive"));
        }
        if tail_rows < 0 || tail_rows >= max_rows {
            return Err(PyValueError::new_err("tail_rows must be between 0 and max_rows - 1"));
        }
        let deadline = match timeout_s {
            None => None,
            Some(s) => Some(
                Duration::try_from_secs_f64(s)
                    .ok()
                    .and_then(|d| Instant::now().checked_add(d))
                    .ok_or_else(|| PyValueError::new_err("timeout_s must be a finite, non-negative number"))?,
            ),
        };
        Ok(AgentRender {
            format,
            max_bytes: max_bytes as usize,
            max_rows: max_rows as usize,
            max_cell_chars: max_cell_chars as usize,
            tail_rows: tail_rows as usize,
            deadline,
            parts: Vec::new(),
            used: 0,
            tables: 0,
            omitted: 0,
        })
    }

    /// Another result set can still be shown.
    fn has_room(&self) -> bool {
        self.room() >= MIN_TABLE_BYTES
    }

    /// Render the statement's current result set. Returns `(ret, exhausted)`:
    /// the ODBC return code (negative on a fetch error, which the caller
    /// raises with `check_error`; nothing is added then) and whether every
    /// row was read, so the next result set can be reached without draining.
    #[pyo3(signature = (statement_handle, messages=None))]
    fn result_set(
        &mut self,
        py: Python<'_>,
        statement_handle: &Bound<'_, PyAny>,
        messages: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<(i16, bool)> {
        let stmt = statement::stmt_of(statement_handle)?;
        let (columns, generation) = statement::result_columns(&stmt)
            .ok_or_else(|| PyRuntimeError::new_err("No result set to render"))?;
        if self.room() < MIN_TABLE_BYTES {
            self.omitted += 1;
            return Ok((SQL_SUCCESS, false));
        }
        let plan = Plan::for_display(&columns).map_err(PyRuntimeError::new_err)?;
        let infos = columns
            .iter()
            .zip(&plan.kinds)
            .map(|(meta, kind)| ColumnInfo {
                name: meta.column_name.clone(),
                sql_type: sqltype::sql_type_name(meta),
                kind: *kind,
                digits: sqltype::fraction_digits(meta),
            })
            .collect();
        self.tables += 1;
        let mut table = Table::new(
            self.format,
            infos,
            Limits {
                max_rows: self.max_rows,
                max_bytes: self.room(),
                max_cell_chars: self.max_cell_chars,
                tail_rows: self.tail_rows,
                result_set: self.tables,
            },
        );
        let deadline = self.deadline;
        let mut info = Vec::new();
        let outcome = py.detach(|| {
            let mut fetch = |want: usize| -> Result<Option<arrow_array::RecordBatch>, i16> {
                let mut sink = ArrowSink::new(&plan, want);
                let fetched = statement::fetch_into(&stmt, &mut sink, want, Some(generation));
                info.extend(fetched.info);
                match fetched.ret {
                    SQL_NO_DATA => Ok(None),
                    ret if ret < 0 => Err(ret),
                    _ => {
                        sink.finish().map(Some).map_err(|_| SQL_ERROR)
                    }
                }
            };
            let mut chunk = FIRST_CHUNK_ROWS;
            loop {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    return Err(None);
                }
                let want = table.want(chunk);
                chunk = (chunk * 2).min(MAX_CHUNK_ROWS);
                if want == 0 {
                    // Head full: one more row proves the result is larger; only
                    // exhaustion proves an exact total.
                    return match fetch(1) {
                        Ok(None) => Ok(true),
                        Ok(Some(batch)) if batch.num_rows() == 0 => Ok(true),
                        Ok(Some(_)) => {
                            table.mark_omitted();
                            Ok(false)
                        }
                        Err(ret) => Err(Some(ret)),
                    };
                }
                match fetch(want) {
                    Ok(None) => return Ok(true),
                    Ok(Some(batch)) => {
                        if !table.push_batch(&batch) {
                            return Ok(false);
                        }
                    }
                    Err(ret) => return Err(Some(ret)),
                }
            }
        });
        if let Some(list) = messages {
            statement::append_messages(py, Some(list), &info)?;
        }
        match outcome {
            Err(Some(ret)) => {
                self.tables -= 1;
                Ok((ret, false))
            }
            Err(None) => Err(PyTimeoutError::new_err("agent total deadline elapsed while fetching rows")),
            Ok(exhausted) => {
                let (text, _complete) = table.finish(exhausted);
                self.push(text);
                Ok((if info.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO }, exhausted))
            }
        }
    }

    /// `-- rows_affected=N` for a statement without a result set.
    fn rows_affected(&mut self, count: i64) {
        self.push_line(format!("-- rows_affected={count}"));
    }

    /// A server message (PRINT, RAISERROR below severity 11, warnings).
    fn message(&mut self, text: &str) {
        let mut line = String::from("-- message: ");
        let room = self.room().saturating_sub(line.len() + 1);
        render::push_escaped_line(&mut line, text, MESSAGE_CHARS, room);
        self.push_line(line);
    }

    /// Count parts that were not read or shown for lack of budget.
    fn omit(&mut self, count: usize) {
        self.omitted += count;
    }

    fn finish(&mut self) -> String {
        if self.parts.is_empty() && self.omitted == 0 {
            return "-- no result set".to_owned();
        }
        if self.omitted > 0 {
            let note = format!("-- {} more parts omitted: raise max_bytes", self.omitted);
            if self.used + 1 + note.len() <= self.max_bytes {
                self.push(note);
            }
        }
        let out = self.parts.join("\n");
        debug_assert!(out.len() <= self.max_bytes);
        out
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AgentRender>()?;
    Ok(())
}
