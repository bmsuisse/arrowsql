"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Row class, which represents a single row of data
from a cursor fetch operation.
"""

import uuid as _uuid
from collections.abc import Mapping
from typing import Any
from arrowsql.ddbc_bindings import RowCore, make_row_fast
from arrowsql.logging import logger


def _apply_converter(converter, value):
    """Run one output converter; on failure log at debug level and keep ``value``.

    ``str`` values are passed as UTF-16-LE bytes (the SQL_WVARCHAR wire format).
    """
    try:
        if isinstance(value, str):
            return converter(value.encode("utf-16-le"))
        return converter(value)
    except Exception:
        logger.debug("Exception occurred in output converter", exc_info=True)
        return value


class Row(RowCore):
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.

    Rows support user-defined attributes, ``vars(row)``, and weak references.
    Internal fields use slots; ``vars(row)`` contains user-defined attributes.

    For dict-like access, use the read-only ``row._mapping`` view (a
    ``collections.abc.Mapping`` of column name -> value). Iterating the Row itself
    (for x in row) yields values, not keys — consistent with pyodbc.Row and
    sqlite3.Row; iterate ``row._mapping`` to get column names.

    Column attribute access behavior depends on the global 'lowercase' setting:
    - When enabled: Case-insensitive attribute access
    - When disabled (default): Case-sensitive attribute access matching original column names

    Example:
        row = cursor.fetchone()
        print(row[0])                  # Access by index
        print(row.column_name)         # Access by column name
        print(dict(row._mapping))      # Convert to a plain dict
        print(row._mapping["col"])     # Access a value by column name via the mapping
        for name in row._mapping:      # Iterate column names
            print(name, row._mapping[name])
        for value in row:              # Iterating the Row yields values, not keys
            print(value)
    """

    __slots__ = ()

    @staticmethod
    def _fast_create(values, column_map, cursor, column_map_lower=None, column_names=None):
        """Construct a Row bypassing __init__ (no converters, no UUID stringification)."""
        return make_row_fast(Row, values, column_map, cursor, column_map_lower, column_names)

    @staticmethod
    def _converted_create(
        values, pairs, column_map, cursor, column_map_lower, column_names, uuid_idx
    ):
        """Construct a Row bypassing __init__ when output converters apply.

        ``pairs`` is ``((column index, converter), ...)`` for the converted columns
        only. Same semantics as ``_apply_output_converters_optimized``: None is
        skipped, str is passed as UTF-16-LE bytes, a failing converter keeps the
        original value. The values are copied once.
        """
        converted = list(values)
        for i, converter in pairs:
            value = values[i]
            if value is None:
                continue
            converted[i] = _apply_converter(converter, value)
        r = make_row_fast(Row, converted, column_map, cursor, column_map_lower, column_names)
        if uuid_idx:
            r._stringify_uuids(uuid_idx)
        return r

    def __init__(
        self,
        values,
        column_map,
        cursor=None,
        converter_map=None,
        uuid_str_indices=None,
        column_map_lower=None,
        column_names=None,
    ):
        """
        Initialize a Row object with values and pre-built column map.
        Args:
            values: List of values for this row
            column_map: Pre-built column name to index mapping (shared across rows)
            cursor: Optional cursor reference (for backward compatibility and lowercase access)
            converter_map: Pre-computed converter map (shared across rows for performance).
                An empty sequence skips converters; None enables the connection fallback.
            uuid_str_indices: Tuple of column indices whose uuid.UUID values should be
                converted to str. Pre-computed once per result set when native_uuid=False.
                None means no conversion (native_uuid=True, the default).
            column_map_lower: Pre-built lowercase column map for O(1) case-insensitive
                lookups. Built once per result set in the cursor when lowercase is enabled;
                None when lowercase is off (the default). Shared across all rows.
            column_names: Canonical, order- and duplicate-preserving column names for
                the result set, snapshotted once by the cursor and shared by reference
                across all rows. Backs ``row._mapping``. None for rows built without a
                cursor snapshot; ``_mapping_keys()`` then reconstructs names from
                ``column_map``.
        """
        if converter_map:
            self._row_values = self._apply_output_converters_optimized(values, converter_map)
        elif (
            converter_map is None
            and cursor
            and hasattr(cursor.connection, "_output_converters")
            and cursor.connection._output_converters
        ):
            # Support direct Row construction without a pre-computed converter map.
            self._row_values = self._apply_output_converters(values, cursor)
        else:
            self._row_values = values

        if uuid_str_indices:
            self._stringify_uuids(uuid_str_indices)

        self._column_map = column_map
        self._row_cursor = cursor
        # Lowercase map is pre-built once per result set in the cursor and shared
        # across all rows. None when lowercase is off (the default) — zero cost.
        self._column_map_lower = column_map_lower
        # Canonical column names for this row's result set, snapshotted once by the
        # cursor (order- and duplicate-preserving) and shared by reference across every
        # row. None only for rows built without a cursor snapshot (e.g. some direct or
        # test constructions); _mapping_keys() then reconstructs names from _column_map.
        self._column_names = column_names

    def _stringify_uuids(self, indices):
        """
        Convert uuid.UUID values at the given column indices to uppercase str in-place.

        This is only called when native_uuid=False. It operates directly on
        self._row_values to avoid creating an extra list copy.
        """
        vals = self._row_values
        # If values are still the original list (no converters), we need a mutable copy
        if not isinstance(vals, list):
            vals = list(vals)
            self._row_values = vals

        for i in indices:
            v = vals[i]
            if v is not None and isinstance(v, _uuid.UUID):
                vals[i] = str(v).upper()

    def _apply_output_converters(self, values, cursor):
        """
        Apply output converters to raw values.

        Args:
            values: Raw values from the database
            cursor: Cursor object with connection and description

        Returns:
            List of converted values
        """
        if not cursor.description:
            return values

        converted_values = list(values)

        for i, (value, desc) in enumerate(zip(values, cursor.description)):
            if desc is None or value is None:
                continue

            # Get SQL type from description
            sql_type = desc[1]  # type_code is at index 1 in description tuple

            # Try to get a converter for this type
            converter = cursor.connection.get_output_converter(sql_type)

            # If no converter found for the SQL type but the value is a string or bytes,
            # try the WVARCHAR converter as a fallback
            if converter is None and isinstance(value, (str, bytes)):
                from arrowsql.constants import ConstantsDDBC

                converter = cursor.connection.get_output_converter(ConstantsDDBC.SQL_WVARCHAR.value)

            # If we found a converter, apply it
            if converter:
                converted_values[i] = _apply_converter(converter, value)

        return converted_values

    def _apply_output_converters_optimized(self, values, converter_map):
        """
        Apply output converters using pre-computed converter map for optimal performance.

        Args:
            values: Raw values from the database
            converter_map: Pre-computed list of converters (one per column, None if no converter)

        Returns:
            List of converted values
        """
        converted_values = list(values)

        for i, (value, converter) in enumerate(zip(values, converter_map)):
            if converter and value is not None:
                converted_values[i] = _apply_converter(converter, value)

        return converted_values


# RowMapping key data per result set: ``id(names)`` -> (names, unique keys, key set).
# Every row of a result set shares one ``_column_names`` tuple, so the
# de-duplicated keys are computed once instead of on every lookup. The entry
# holds ``names`` itself, so an id can only match while that tuple is alive.
_MAPPING_KEYS_CACHE: dict = {}
_MAPPING_KEYS_CACHE_MAX = 64


def _mapping_key_entry(row: "Row"):
    names = row._column_names
    if names is None:
        unique = tuple(dict.fromkeys(row._mapping_keys()))
        return (None, unique, frozenset(unique))
    entry = _MAPPING_KEYS_CACHE.get(id(names))
    if entry is not None and entry[0] is names:
        return entry
    unique = tuple(dict.fromkeys(names))
    entry = (names, unique, frozenset(unique))
    if len(_MAPPING_KEYS_CACHE) >= _MAPPING_KEYS_CACHE_MAX:
        _MAPPING_KEYS_CACHE.clear()
    _MAPPING_KEYS_CACHE[id(names)] = entry
    return entry


class RowMapping(Mapping):
    """Read-only ``Mapping`` view over a :class:`Row` (column name -> value).

    Created via :attr:`Row._mapping`. Keys are the row's canonical column names,
    order-preserving and de-duplicated (last column wins for a repeated name).
    Lookup and membership use those names exactly -- no case-insensitive or catalog
    alias resolution -- so iteration, ``in`` and ``[]`` always agree. The view
    reflects the row it wraps and copies no values.
    """

    __slots__ = ("_row",)

    def __init__(self, row: "Row") -> None:
        self._row = row

    def __getitem__(self, key: str) -> Any:
        # Restrict lookups to the canonical column names yielded by __iter__ so
        # membership and lookup agree with iteration (proper Mapping semantics).
        # Row.__getitem__ additionally accepts case-insensitive names and catalog
        # aliases, but those are not iterated keys, so the view must not resolve
        # them here.
        if isinstance(key, str) and key in _mapping_key_entry(self._row)[2]:
            return self._row[key]
        raise KeyError(key)

    def __iter__(self):
        return iter(_mapping_key_entry(self._row)[1])

    def __len__(self) -> int:
        return len(_mapping_key_entry(self._row)[1])

    def __repr__(self) -> str:
        return f"RowMapping({dict(self)!r})"
