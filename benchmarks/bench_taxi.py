"""Superseded by benchmarks/run.py: one driver per process is now `benchmarks/child.py`.

    python benchmarks/run.py --driver <mssql_python|rust_port|fastmssql|...> [--quick]

Old workload names map to the new matrix as documented in benchmarks/README.md
("Compatibility with bench_taxi.py / compare.py").
"""

import sys

sys.exit(__doc__)
