"""A fetch of a few large values must not reserve arena room for the whole row hint."""

import re

import pytest


def _vm_peak_mb():
    with open("/proc/self/status") as f:
        return int(re.search(r"VmPeak:\s+(\d+) kB", f.read()).group(1)) / 1024


@pytest.mark.skipif(not __import__("os").path.exists("/proc/self/status"), reason="needs /proc")
@pytest.mark.parametrize(
    "expr",
    [
        "CAST(REPLICATE(CAST(0x41 AS varbinary(max)), 1048576) AS varbinary(max))",
        "CAST(REPLICATE(CAST('a' AS nvarchar(max)), 131072) AS nvarchar(max))",
    ],
    ids=["varbinary_1MiB", "nvarchar_128K"],
)
def test_few_large_values_do_not_reserve_whole_row_hint(cursor, expr):
    cursor.execute(f"SELECT {expr} AS v UNION ALL SELECT {expr}")
    before = _vm_peak_mb()
    rows = cursor.fetchall()
    grown = _vm_peak_mb() - before
    assert len(rows) == 2
    assert grown < 200, f"fetchall of 2 large values grew the peak address space by {grown:.0f} MiB"
