"""Compatibility alias: ``import mssql_python`` loads :mod:`arrowsql`.

``mssql_python`` and every ``mssql_python.X`` are the same module objects as
``arrowsql`` and ``arrowsql.X``, so code written for microsoft/mssql-python
(imports, ``isinstance`` checks, monkeypatching) runs unchanged.
"""

import importlib
import importlib.util
import sys

_PREFIX = __name__ + "."


class _Alias:
    """Meta-path finder and loader (no importlib.abc: it imports tempfile)."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(_PREFIX):
            return importlib.util.spec_from_loader(fullname, self)
        return None

    def create_module(self, spec):
        return importlib.import_module("arrowsql." + spec.name[len(_PREFIX):])

    def exec_module(self, module):
        pass


sys.meta_path.insert(0, _Alias())
import arrowsql  # noqa: E402

# Register the submodules arrowsql already loaded. Importing one through the
# finder would set it on the package again and shadow a same-named function
# (``arrowsql.pooling`` is a function that hides the ``pooling`` module).
for _name, _module in list(sys.modules.items()):
    if _name.startswith("arrowsql."):
        sys.modules.setdefault(_PREFIX + _name[len("arrowsql."):], _module)
sys.modules[__name__] = arrowsql
