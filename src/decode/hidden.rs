//! Hidden result columns (`fHidden` in COLMETADATA: FOR BROWSE keys, the
//! ROWSTAT of a T-SQL `FETCH`). msodbcsql never exposes them, so the column
//! list handed out drops them and row sinks are wrapped to skip their values.
//! The wrapper is only built when a result has a hidden column; other results
//! decode into the sink directly.

use std::borrow::Cow;
use std::mem::MaybeUninit;

use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::row_writer::{RowWriter, ValueKind};
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::EncodingType;
use mssql_tds::datatypes::sql_vector::SqlVector;
use mssql_tds::datatypes::sqldatatypes::TdsDataType;
use mssql_tds::query::metadata::ColumnMetadata;
use uuid::Uuid;

const HIDDEN: u32 = u32::MAX;

/// The columns a user sees: the wire columns minus the hidden ones.
pub fn visible_columns(columns: &[ColumnMetadata]) -> Vec<ColumnMetadata> {
    columns.iter().filter(|c| !c.is_hidden()).cloned().collect()
}

/// Wire column index -> visible index (`HIDDEN` when dropped); `None` when no
/// column is hidden.
pub fn visible_map(columns: &[ColumnMetadata]) -> Option<Vec<u32>> {
    if !columns.iter().any(ColumnMetadata::is_hidden) {
        return None;
    }
    let mut next = 0u32;
    Some(
        columns
            .iter()
            .map(|c| {
                if c.is_hidden() {
                    HIDDEN
                } else {
                    next += 1;
                    next - 1
                }
            })
            .collect(),
    )
}

/// A [`RowWriter`] that forwards only the visible columns, renumbered.
pub struct HiddenFilter<'a, W> {
    pub inner: &'a mut W,
    map: &'a [u32],
}

impl<'a, W> HiddenFilter<'a, W> {
    pub fn new(inner: &'a mut W, map: &'a [u32]) -> Self {
        Self { inner, map }
    }
}

macro_rules! forward {
    ($($name:ident($ty:ty)),* $(,)?) => {$(
        fn $name(&mut self, col: usize, val: $ty) {
            if let Some(&v) = self.map.get(col) && v != HIDDEN {
                self.inner.$name(v as usize, val);
            }
        }
    )*};
}

impl<W: RowWriter> RowWriter for HiddenFilter<'_, W> {
    fn write_null(&mut self, col: usize) {
        if let Some(&v) = self.map.get(col) && v != HIDDEN {
            self.inner.write_null(v as usize);
        }
    }
    forward!(
        write_bool(bool), write_u8(u8), write_i16(i16), write_i32(i32), write_i64(i64),
        write_f32(f32), write_f64(f64), write_decimal(DecimalParts), write_numeric(DecimalParts),
        write_date(SqlDate), write_time(SqlTime), write_datetime(SqlDateTime),
        write_smalldatetime(SqlSmallDateTime), write_datetime2(SqlDateTime2),
        write_datetimeoffset(SqlDateTimeOffset), write_money(SqlMoney),
        write_smallmoney(SqlSmallMoney), write_uuid(Uuid), write_xml(SqlXml),
        write_json(SqlJson), write_vector(SqlVector),
    );
    fn write_string(&mut self, col: usize, bytes: Cow<'_, [u8]>, encoding_type: EncodingType) {
        if let Some(&v) = self.map.get(col) && v != HIDDEN {
            self.inner.write_string(v as usize, bytes, encoding_type);
        }
    }
    fn write_bytes(&mut self, col: usize, bytes: Cow<'_, [u8]>) {
        if let Some(&v) = self.map.get(col) && v != HIDDEN {
            self.inner.write_bytes(v as usize, bytes);
        }
    }
    fn write_variant_base_type(&mut self, col: usize, base: TdsDataType) {
        if let Some(&v) = self.map.get(col) && v != HIDDEN {
            self.inner.write_variant_base_type(v as usize, base);
        }
    }
    fn end_row(&mut self) {
        self.inner.end_row();
    }
    fn value_destination<'b>(
        &'b mut self,
        col: usize,
        kind: ValueKind<'_>,
        length: usize,
    ) -> Option<&'b mut [MaybeUninit<u8>]> {
        match self.map.get(col) {
            Some(&v) if v != HIDDEN => self.inner.value_destination(v as usize, kind, length),
            _ => None,
        }
    }
    fn commit_value(&mut self, col: usize, complete: bool) {
        if let Some(&v) = self.map.get(col) && v != HIDDEN {
            self.inner.commit_value(v as usize, complete);
        }
    }
}
