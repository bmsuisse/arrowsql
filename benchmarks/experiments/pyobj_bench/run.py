"""Print ns/object for each technique (best of 5, n=1M; decimals n=200k)."""

import json

import pyobj_bench

KINDS = [
    "int_small", "int_large", "float",
    "str_utf8_1char", "str_interned_1char", "str_utf8_34", "str_utf16_decode_34", "str_ascii_fastpath_34",
    "decimal_from_str", "decimal_from_tuple", "decimal_from_int_scaleb",
    "datetime_capi", "tuple20_of_none", "list20_of_none",
]
for kind in KINDS:
    n = 200_000 if kind.startswith("decimal") else 1_000_000
    best = min(pyobj_bench.per_object(kind, n) for _ in range(5))
    print(json.dumps({"kind": kind, "ns_per_object": round(best, 1)}))
