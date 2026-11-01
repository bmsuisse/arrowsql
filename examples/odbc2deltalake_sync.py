"""Sync a SQL Server table to Delta Lake with odbc2deltalake, reading through arrowsql.

odbc2deltalake (https://github.com/bmsuisse/odbc2deltalake) writes SCD2-style Delta
tables from SQL Server; its default reader uses arrow-odbc. This reader swaps in
arrowsql for the three places it reads from the server, so no ODBC driver is
needed and source reads stream as Arrow batches (strings as UTF-8, no
20,000-character cut-off on long text).

    pip install arrowsql "odbc2deltalake[local]" "deltalake<1"   # odbc2deltalake#93
    python examples/odbc2deltalake_sync.py
"""

import re
from pathlib import Path
from typing import Literal, Union

import pyarrow as pa
import sqlglot.expressions as ex
from odbc2deltalake import WriteConfig, write_db_to_delta
from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.read_utils.delta_rs import _all_nullable, _build_type, _get_type
from odbc2deltalake.reader.odbc_reader import ODBCReader

import arrowsql
from _demo import TABLE, connection_string, create_demo_table, drop_demo_table


class ArrowSQLReader(ODBCReader):
    """odbc2deltalake reader whose source queries run through arrowsql."""

    def __init__(self, connection_string: str, local_db: str = ":memory:") -> None:
        # arrowsql speaks TDS itself, so an ODBC "Driver=" keyword is not accepted.
        connection_string = re.sub(r"(?i)\bdriver\s*=\s*(\{[^}]*\}|[^;]*);?", "", connection_string)
        super().__init__(connection_string, local_db=local_db)

    def _reader(self, sql: str) -> pa.RecordBatchReader:
        # A pyarrow reader: odbc2deltalake reads `.schema` and casts batch by batch.
        return pa.RecordBatchReader.from_stream(arrowsql.read_arrow_stream(self.connection_string, sql))

    def source_schema_limit_one(self, sql: ex.Query):
        from odbc2deltalake.metadata import InformationSchemaColInfo

        schema = self._reader(sql.limit(0).sql(self.source_dialect)).schema
        return [
            InformationSchemaColInfo(
                column_name=name,
                data_type_str=str(schema.field(name).type),
                data_type=_build_type(_get_type(schema.field(name).type)),
                is_nullable=schema.field(name).nullable,
            )
            for name in schema.names
        ]

    def source_sql_to_py(self, sql: Union[str, ex.Query]) -> list[dict]:
        if isinstance(sql, ex.Query):
            sql = sql.sql(self.source_dialect)
        return pa.table(arrowsql.read_arrow(self.connection_string, sql)).to_pylist()

    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        reader = self._reader(sql)
        path, options = delta_path.as_path_options(flavor="object_store")
        cast_schema, schema_mode = self._handle_schema_drift(delta_path, allow_schema_drift, mode, reader.schema)
        if cast_schema:
            reader = pa.RecordBatchReader.from_batches(cast_schema, (b.cast(cast_schema) for b in reader))
        try:
            write_deltalake(
                path,
                reader,
                schema=_all_nullable(reader.schema),
                mode=mode,
                writer_properties=self.writer_properties,
                schema_mode=schema_mode,
                storage_options=options,
            )
        except DeltaError as e:
            if "No data source supplied to write command" not in str(e):
                raise
            if mode == "overwrite":
                self._write_empty_delta_table(reader.schema, path, options)


conn_str = connection_string()
create_demo_table(conn_str)
schema, table = TABLE.split(".")

# Full load first, then incremental runs merge changes by primary key into the same Delta table.
target = Path("delta_out") / table
write_db_to_delta(ArrowSQLReader(conn_str), (schema, table), target, WriteConfig(primary_keys=["trip_id"]))
print(f"synced {TABLE} to {target}")

drop_demo_table(conn_str)
