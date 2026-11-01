"""RowCore participates in the cyclic GC (review r5 F4) and the verified-subclass
cache does not pin classes forever (F16). No database needed."""

import gc
import weakref

from arrowsql.ddbc_bindings import make_row_fast
from mssql_python.row import Row


def _freed(ref):
    gc.collect()
    return ref() is None


def test_cycle_through_values_is_collected():
    values = []
    row = Row(values, {}, converter_map=[])
    values.append(row)
    ref = weakref.ref(row)
    del values, row
    assert _freed(ref)


def test_cycle_through_each_field_is_collected():
    for field in ("_column_map", "_row_cursor", "_column_map_lower", "_column_names"):
        row = Row([], {}, converter_map=[])
        holder = [row]
        setattr(row, field, holder)
        ref = weakref.ref(row)
        del row, holder
        assert _freed(ref), field


def test_cycle_through_instance_dict_is_collected():
    row = Row([], {}, converter_map=[])
    row.me = row
    ref = weakref.ref(row)
    del row
    assert _freed(ref)


def test_make_row_fast_cycle_is_collected():
    values = []
    row = make_row_fast(Row, values, {}, None, None, None)
    values.append(row)
    ref = weakref.ref(row)
    del values, row
    assert _freed(ref)


def test_verified_subclass_is_not_kept_alive():
    class Sub(Row):
        __slots__ = ()

    row = make_row_fast(Sub, [1], {}, None, None, None)
    assert row[0] == 1
    ref = weakref.ref(Sub)
    del row, Sub
    assert _freed(ref)


def test_override_in_subclass_body_is_honoured():
    class Sub(Row):
        __slots__ = ()
        _row_values = property(lambda self: [9], lambda self, v: seen.append(v))

    seen = []
    make_row_fast(Sub, [1], {}, None, None, None)
    assert seen == [[1]]
