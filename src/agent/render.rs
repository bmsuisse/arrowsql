//! Bounded text rendering of one result set: a typed header, escaped rows, a
//! footer. Everything, footer included, stays inside the table's byte limit;
//! a row that would not fit is dropped whole, never cut.

use std::collections::VecDeque;
use std::fmt::{self, Write};

use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Time64NanosecondType,
    TimestampMicrosecondType, UInt8Type,
};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    RecordBatch,
};

use super::fmt as text;
use crate::arrow::plan::Kind;

const CLIPPED: &str = "…[truncated]";

/// Text layout of a preview.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Format {
    /// Tab-separated, `name:type` header. Backslash, tab, newline and carriage
    /// return are backslash-escaped, NULL is `\N` and the empty string is an
    /// empty field (PostgreSQL `COPY` text), so a cell never holds a delimiter.
    #[default]
    Tsv,
    /// Pipe table without padding; text is quoted and Markdown-escaped.
    Markdown,
}

impl Format {
    pub fn parse(name: &str) -> Option<Format> {
        match name {
            "tsv" => Some(Format::Tsv),
            "markdown" => Some(Format::Markdown),
            _ => None,
        }
    }
}

/// What the header shows for one column, and how its cells are formatted.
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: String,
    pub kind: Kind,
    /// Fractional-second digits for temporal kinds.
    pub digits: u32,
}

pub struct Limits {
    pub max_rows: usize,
    /// Everything this table may emit, footer included.
    pub max_bytes: usize,
    pub max_cell_chars: usize,
    pub tail_rows: usize,
    /// 1-based position among the result sets; shown in the footer from 2 on.
    pub result_set: usize,
}

/// Longest footer the table can produce, so rows never crowd it out.
fn footer_reserve(limits: &Limits) -> usize {
    let big = 999_999_999_999;
    Table::footer_text(limits, big, big, false, true, true, big).len()
}

enum Phase {
    Head,
    Tail,
}

pub struct Table {
    format: Format,
    columns: Vec<ColumnInfo>,
    limits: Limits,
    /// Bytes available to header and rows.
    body_limit: usize,
    head_limit: usize,
    head_rows: usize,
    out: String,
    scratch: String,
    rows: usize,
    phase: Phase,
    /// Rows past the head, seen in tail mode.
    after: usize,
    ring: VecDeque<RecordBatch>,
    ring_rows: usize,
    omitted: bool,
    clipped: bool,
    full: bool,
}

/// A typed view of one column of a batch.
enum Col<'a> {
    U8(&'a PrimitiveArray<UInt8Type>),
    I16(&'a PrimitiveArray<Int16Type>),
    I32(&'a PrimitiveArray<Int32Type>),
    I64(&'a PrimitiveArray<Int64Type>),
    F32(&'a PrimitiveArray<Float32Type>),
    F64(&'a PrimitiveArray<Float64Type>),
    Bool(&'a BooleanArray),
    Decimal(&'a Decimal128Array, u32),
    Timestamp(&'a PrimitiveArray<TimestampMicrosecondType>, u32, bool),
    Date(&'a PrimitiveArray<Date32Type>),
    Time(&'a PrimitiveArray<Time64NanosecondType>, u32),
    Text(&'a LargeStringArray),
    Binary(&'a LargeBinaryArray),
}

impl<'a> Col<'a> {
    fn new(array: &'a dyn Array, column: &ColumnInfo) -> Col<'a> {
        fn get<T: 'static>(array: &dyn Array) -> &T {
            array.as_any().downcast_ref().expect("column storage always matches its kind")
        }
        match column.kind {
            Kind::UInt8 => Col::U8(get(array)),
            Kind::Int16 => Col::I16(get(array)),
            Kind::Int32 => Col::I32(get(array)),
            Kind::Int64 => Col::I64(get(array)),
            Kind::Float32 => Col::F32(get(array)),
            Kind::Float64 => Col::F64(get(array)),
            Kind::Bool => Col::Bool(get(array)),
            Kind::Decimal { scale, .. } => {
                Col::Decimal(get(array), scale.max(0) as u32)
            }
            Kind::Timestamp { utc } => {
                Col::Timestamp(get(array), column.digits, utc)
            }
            Kind::Date32 => Col::Date(get(array)),
            Kind::Time64Ns => Col::Time(get(array), column.digits),
            Kind::Text => Col::Text(get(array)),
            Kind::Binary => Col::Binary(get(array)),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Col::U8(a) => a.is_null(row),
            Col::I16(a) => a.is_null(row),
            Col::I32(a) => a.is_null(row),
            Col::I64(a) => a.is_null(row),
            Col::F32(a) => a.is_null(row),
            Col::F64(a) => a.is_null(row),
            Col::Bool(a) => a.is_null(row),
            Col::Decimal(a, _) => a.is_null(row),
            Col::Timestamp(a, ..) => a.is_null(row),
            Col::Date(a) => a.is_null(row),
            Col::Time(a, _) => a.is_null(row),
            Col::Text(a) => a.is_null(row),
            Col::Binary(a) => a.is_null(row),
        }
    }
}

/// Writes escaped text into a `String`, stopping at a character or byte limit.
struct EscapedWriter<'a> {
    output: &'a mut String,
    chars: usize,
    bytes: usize,
    clipped: bool,
    tsv: bool,
}

impl<'a> EscapedWriter<'a> {
    fn new(output: &'a mut String, chars: usize, bytes: usize, tsv: bool) -> Self {
        Self { output, chars, bytes, clipped: false, tsv }
    }

    fn raw(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.bytes {
            self.clipped = true;
            return Err(fmt::Error);
        }
        self.bytes -= text.len();
        self.output.push_str(text);
        Ok(())
    }

    /// Whether `s` can be copied verbatim: no escaping, within both limits.
    fn plain(&self, s: &str) -> bool {
        s.len() <= self.chars.min(self.bytes)
            && s.bytes().all(|b| {
                b >= 0x20
                    && b < 0x7f
                    && b != b'\\'
                    && (self.tsv || !b"&<>|`*_[]~\"".contains(&b))
            })
    }
}

impl Write for EscapedWriter<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.plain(text) {
            self.chars -= text.len();
            self.bytes -= text.len();
            self.output.push_str(text);
            return Ok(());
        }
        for ch in text.chars() {
            if self.chars == 0 {
                self.clipped = true;
                return Err(fmt::Error);
            }
            match ch {
                '&' | '<' | '>' | '|' | '`' | '*' | '_' | '[' | ']' | '~' | '"' if self.tsv => {
                    self.raw(ch.encode_utf8(&mut [0; 4]))?
                }
                '&' => self.raw("&amp;")?,
                '<' => self.raw("&lt;")?,
                '>' => self.raw("&gt;")?,
                '\n' => self.raw("\\n")?,
                '\r' => self.raw("\\r")?,
                '\t' => self.raw("\\t")?,
                '\\' | '|' | '`' | '*' | '_' | '[' | ']' | '~' | '"' => {
                    self.raw("\\")?;
                    self.raw(ch.encode_utf8(&mut [0; 4]))?;
                }
                c if c.is_control() => {
                    let mut escaped = [0_u8; 10];
                    let mut at = escaped.len();
                    let mut code = c as u32;
                    while code != 0 || escaped.len() - at < 4 {
                        at -= 1;
                        escaped[at] = b"0123456789abcdef"[(code & 15) as usize];
                        code >>= 4;
                    }
                    at -= 2;
                    escaped[at..at + 2].copy_from_slice(b"\\u");
                    self.raw(std::str::from_utf8(&escaped[at..]).unwrap())?;
                }
                c => self.raw(c.encode_utf8(&mut [0; 4]))?,
            }
            self.chars -= 1;
        }
        Ok(())
    }
}

/// Escape `text` (a message or error line) into `out`, clipped to `max_chars`.
pub fn push_escaped_line(out: &mut String, text: &str, max_chars: usize, max_bytes: usize) -> bool {
    let reserve = CLIPPED.len();
    let mut writer = EscapedWriter::new(out, max_chars, max_bytes.saturating_sub(reserve), true);
    let _ = writer.write_str(text);
    let clipped = writer.clipped;
    if clipped {
        out.push_str(CLIPPED);
    }
    clipped
}

enum Row {
    Fits { clipped: bool },
    NoFit,
}

impl Table {
    pub fn new(format: Format, columns: Vec<ColumnInfo>, limits: Limits) -> Table {
        let reserve = footer_reserve(&limits);
        let body_limit = limits.max_bytes.saturating_sub(reserve);
        // Keep a share for the tail sample when one is requested.
        let tail = limits.tail_rows.min(limits.max_rows.saturating_sub(1));
        let (head_limit, head_rows) = if tail > 0 {
            (body_limit - body_limit / 3, limits.max_rows - tail)
        } else {
            (body_limit, limits.max_rows)
        };
        let mut table = Table {
            format,
            columns,
            limits,
            body_limit,
            head_limit,
            head_rows,
            out: String::with_capacity(body_limit.min(16 * 1024)),
            scratch: String::new(),
            rows: 0,
            phase: Phase::Head,
            after: 0,
            ring: VecDeque::new(),
            ring_rows: 0,
            omitted: false,
            clipped: false,
            full: false,
        };
        table.header();
        table
    }

    fn header(&mut self) {
        let tsv = self.format == Format::Tsv;
        let mut header = String::new();
        let mut writer = EscapedWriter::new(&mut header, usize::MAX, usize::MAX, tsv);
        if self.columns.is_empty() {
            let _ = writer.write_str("Result has no columns.");
        } else if tsv {
            for (index, column) in self.columns.iter().enumerate() {
                if index > 0 {
                    let _ = writer.raw("\t");
                }
                let _ = writer.write_str(&column.name);
                let _ = writer.raw(":");
                let _ = writer.write_str(&column.sql_type);
            }
        } else {
            let _ = writer.raw("|");
            for column in &self.columns {
                let _ = writer.write_str(&column.name);
                let _ = writer.raw(" (");
                let _ = writer.write_str(&column.sql_type);
                let _ = writer.raw(")|");
            }
            let _ = writer.raw("\n|");
            for _ in &self.columns {
                let _ = writer.raw("-|");
            }
        }
        header.push('\n');
        if header.len() > self.head_limit {
            self.out.push_str("Result schema exceeds max_bytes; increase max_bytes.\n");
            self.full = true;
            self.omitted = true;
        } else {
            self.out.push_str(&header);
        }
    }

    /// How many rows to fetch next; 0 when the head is full and no tail is sampled.
    pub fn want(&mut self, chunk: usize) -> usize {
        if self.tail_mode() && (self.full || self.rows >= self.head_rows) {
            self.phase = Phase::Tail;
        }
        match self.phase {
            // Rows past the head are only counted and kept, not formatted.
            Phase::Tail => chunk * 16,
            Phase::Head if self.full => 0,
            Phase::Head => (self.head_rows - self.rows).min(chunk),
        }
    }

    pub fn tail_mode(&self) -> bool {
        self.limits.tail_rows.min(self.limits.max_rows.saturating_sub(1)) > 0
    }

    /// The head is full and more rows are known to exist.
    pub fn mark_omitted(&mut self) {
        self.omitted = true;
    }

    /// Append the rows of `batch`; false once nothing more is wanted.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> bool {
        if self.columns.is_empty() {
            let take = batch.num_rows().min(self.limits.max_rows - self.rows);
            self.rows += take;
            self.omitted |= take < batch.num_rows();
            return self.rows < self.limits.max_rows;
        }
        let tsv = self.format == Format::Tsv;
        let columns = &self.columns;
        let cols: Vec<Col> =
            batch.columns().iter().zip(columns).map(|(a, c)| Col::new(a.as_ref(), c)).collect();
        let mut tail_from = None;
        for row in 0..batch.num_rows() {
            if matches!(self.phase, Phase::Head) {
                let fits = if self.rows >= self.head_rows || self.full {
                    false
                } else {
                    let available = self.head_limit - self.out.len();
                    match render_row(
                        &mut self.scratch,
                        &cols,
                        row,
                        tsv,
                        self.limits.max_cell_chars,
                        available,
                    ) {
                        Row::Fits { clipped } => {
                            self.out.push_str(&self.scratch);
                            self.clipped |= clipped;
                            self.rows += 1;
                            true
                        }
                        Row::NoFit => {
                            self.full = true;
                            false
                        }
                    }
                };
                if fits {
                    continue;
                }
                if !self.tail_mode() {
                    self.omitted = true;
                    return false;
                }
                self.phase = Phase::Tail;
            }
            tail_from.get_or_insert(row);
            self.after += 1;
        }
        if let Some(start) = tail_from {
            let kept = batch.slice(start, batch.num_rows() - start);
            self.ring_rows += kept.num_rows();
            self.ring.push_back(kept);
            while self.ring.len() > 1
                && self.ring_rows - self.ring[0].num_rows() >= self.limits.tail_rows
            {
                self.ring_rows -= self.ring.pop_front().unwrap().num_rows();
            }
        }
        true
    }

    /// The footer for these counts; also the worst case for [`footer_reserve`].
    fn footer_text(
        limits: &Limits,
        shown: usize,
        total: usize,
        exact: bool,
        clipped: bool,
        sampled: bool,
        omitted_rows: usize,
    ) -> String {
        let complete = exact && !clipped && !sampled;
        let mut footer = String::from("\n");
        if limits.result_set > 1 {
            let _ = write!(footer, "result_set={}; ", limits.result_set);
        }
        let _ = write!(
            footer,
            "rows_shown={shown}; complete={complete}; total_rows{}{total}",
            if exact { "=" } else { ">=" }
        );
        if sampled {
            let _ = write!(footer, "; sample=head+tail; rows_omitted={omitted_rows}");
        }
        if clipped {
            footer.push_str("; cells_truncated=true");
        }
        if !complete {
            footer.push_str("; next=filter/aggregate SQL or raise budgets");
        }
        footer
    }

    /// Finish the table. `exhausted`: the source has no more rows.
    pub fn finish(mut self, exhausted: bool) -> (String, bool) {
        let mut sampled = false;
        let mut omitted_rows = 0;
        let mut clipped = self.clipped;
        let mut shown = self.rows;
        if self.after > 0 && exhausted {
            let (lines, tail_clipped) = self.render_tail();
            if !lines.is_empty() {
                omitted_rows = self.after - lines.len();
                let _ = writeln!(self.out, "… {omitted_rows} rows omitted …");
                for line in &lines {
                    self.out.push_str(line);
                }
                shown += lines.len();
                clipped |= tail_clipped;
                sampled = true;
            } else {
                self.omitted = true;
            }
        } else if self.after > 0 {
            self.omitted = true;
        }
        let exact = exhausted && !self.omitted;
        let total = if sampled { self.rows + self.after } else { self.rows };
        let footer = Table::footer_text(&self.limits, shown, total, exact || sampled, clipped, sampled, omitted_rows);
        self.out.push_str(&footer);
        debug_assert!(self.out.len() <= self.limits.max_bytes);
        let complete = exact && !clipped && !sampled;
        (self.out, complete)
    }

    /// The newest rows that fit after the head, oldest first.
    fn render_tail(&mut self) -> (Vec<String>, bool) {
        let tsv = self.format == Format::Tsv;
        let marker = 48;
        let room = self
            .body_limit
            .saturating_sub(self.out.len())
            .saturating_sub(marker);
        let wanted = self.limits.tail_rows.min(self.limits.max_rows - self.rows).min(self.after);
        let mut lines: Vec<String> = Vec::new();
        let mut clipped = false;
        let mut left = wanted;
        let mut scratch = String::new();
        'batches: for batch in self.ring.iter().rev() {
            let cols: Vec<Col> = batch
                .columns()
                .iter()
                .zip(&self.columns)
                .map(|(a, c)| Col::new(a.as_ref(), c))
                .collect();
            for row in (0..batch.num_rows()).rev() {
                if left == 0 {
                    break 'batches;
                }
                match render_row(
                    &mut scratch,
                    &cols,
                    row,
                    tsv,
                    self.limits.max_cell_chars,
                    room,
                ) {
                    Row::Fits { clipped: c } => {
                        clipped |= c;
                        lines.push(scratch.clone());
                    }
                    Row::NoFit => break 'batches,
                }
                left -= 1;
            }
        }
        lines.reverse();
        // Drop the oldest rows until the sample fits next to the head.
        let mut size: usize = lines.iter().map(String::len).sum();
        let mut skip = 0;
        while size > room && skip < lines.len() {
            size -= lines[skip].len();
            skip += 1;
        }
        lines.drain(..skip);
        (lines, clipped)
    }
}

/// Render row `row` into `scratch`; `NoFit` if it exceeds `available` bytes.
fn render_row(
    scratch: &mut String,
    cols: &[Col],
    row: usize,
    tsv: bool,
    max_cell_chars: usize,
    available: usize,
) -> Row {
    scratch.clear();
    if !tsv {
        scratch.push('|');
    }
    let mut row_clipped = false;
    let mut cell = String::new();
    for (index, col) in cols.iter().enumerate() {
        // Closing quote, clipping marker and delimiter must still fit.
        let reserve = CLIPPED.len() + 5;
        if scratch.len() + reserve + 2 > available {
            return Row::NoFit;
        }
        if tsv && index > 0 {
            scratch.push('\t');
        }
        let null = col.is_null(row);
        let quote = !tsv && !null && matches!(col, Col::Text(_));
        if quote {
            scratch.push('"');
        }
        let cell_bytes = available - scratch.len() - reserve;
        let mut sink = EscapedWriter::new(scratch, max_cell_chars, cell_bytes, tsv);
        let mut forced_clip = false;
        let result = if null {
            sink.chars = usize::MAX;
            sink.bytes = sink.bytes.max(4);
            sink.raw(if tsv { "\\N" } else { "NULL" })
        } else {
            cell.clear();
            match col {
                Col::U8(a) => write!(cell, "{}", a.value(row)),
                Col::I16(a) => write!(cell, "{}", a.value(row)),
                Col::I32(a) => write!(cell, "{}", a.value(row)),
                Col::I64(a) => write!(cell, "{}", a.value(row)),
                Col::F32(a) => write!(cell, "{}", a.value(row)),
                Col::F64(a) => write!(cell, "{}", a.value(row)),
                Col::Bool(a) => write!(cell, "{}", u8::from(a.value(row))),
                Col::Decimal(a, scale) => {
                    text::write_decimal(&mut cell, a.value(row), *scale);
                    Ok(())
                }
                Col::Timestamp(a, digits, utc) => {
                    text::write_timestamp(&mut cell, a.value(row), *digits);
                    if *utc {
                        cell.push_str("+00:00");
                    }
                    Ok(())
                }
                Col::Date(a) => {
                    text::write_date(&mut cell, i64::from(a.value(row)));
                    Ok(())
                }
                Col::Time(a, digits) => {
                    text::write_time(&mut cell, a.value(row), *digits);
                    Ok(())
                }
                Col::Binary(a) => {
                    let bytes = a.value(row);
                    // Hex doubles the size: only render what the cell can show.
                    let keep = bytes.len().min(max_cell_chars / 2 + 1);
                    forced_clip = keep < bytes.len();
                    text::write_hex(&mut cell, &bytes[..keep]);
                    Ok(())
                }
                Col::Text(_) => Ok(()),
            }
            .ok();
            match col {
                Col::Text(a) => sink.write_str(a.value(row)),
                _ => sink.write_str(&cell),
            }
        };
        let clipped = sink.clipped || (forced_clip && result.is_ok());
        if clipped {
            scratch.push_str(CLIPPED);
            row_clipped = true;
        }
        if quote {
            scratch.push('"');
        }
        if !tsv {
            scratch.push('|');
        }
    }
    scratch.push('\n');
    if scratch.len() > available {
        return Row::NoFit;
    }
    Row::Fits { clipped: row_clipped }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escaped(text: &str, chars: usize, bytes: usize, tsv: bool) -> (String, bool) {
        let mut out = String::new();
        let mut writer = EscapedWriter::new(&mut out, chars, bytes, tsv);
        let _ = writer.write_str(text);
        let clipped = writer.clipped;
        (out, clipped)
    }

    #[test]
    fn tsv_escapes_delimiters_and_keeps_markdown_text_raw() {
        assert_eq!(escaped("a\tb\nc\\d|e", 100, 100, true).0, "a\\tb\\nc\\\\d|e");
        assert_eq!(escaped("a|b_c", 100, 100, false).0, "a\\|b\\_c");
    }

    #[test]
    fn clipping_never_splits_a_character() {
        let (out, clipped) = escaped("é🦀é🦀", 100, 5, true);
        assert!(clipped);
        assert_eq!(out, "é");
        assert_eq!(escaped("é🦀é🦀", 100, 6, true).0, "é🦀");
        let (out, clipped) = escaped("é🦀é🦀", 3, 100, true);
        assert!(clipped);
        assert_eq!(out, "é🦀é");
    }

    #[test]
    fn every_control_character_escapes_to_one_ascii_sequence() {
        for code in 0..=0x10ffff_u32 {
            if let Some(ch) = char::from_u32(code)
                && ch.is_control()
            {
                let (out, _) = escaped(ch.encode_utf8(&mut [0; 4]), 1, 64, true);
                let expected = match ch {
                    '\t' => "\\t".to_owned(),
                    '\n' => "\\n".to_owned(),
                    '\r' => "\\r".to_owned(),
                    _ => format!("\\u{code:04x}"),
                };
                assert_eq!(out, expected);
            }
        }
    }
}
