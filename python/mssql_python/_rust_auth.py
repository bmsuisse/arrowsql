"""
Authentication= handling for the Rust core (RUST-PORT, new module).

The Rust core logs in with exactly what the connection string says; it has no
msodbcsql underneath that interprets ``Authentication=``. Upstream's auth.py
returns ``None`` for the modes ODBC "handles natively" (ServicePrincipal,
Windows Interactive) and does not know ``SqlPassword`` / ``ActiveDirectoryPassword``
/ ``ActiveDirectoryIntegrated`` at all, so without this module those would fall
through to a SQL login with the client id or UPN. This module keeps auth.py
(and the tests that pin its return values) unchanged and makes every
``Authentication`` value either work or fail with a clear error.
"""

from __future__ import annotations

import sys
from typing import Callable, Dict, Optional

from mssql_python.constants import (
    AuthType,
    _KEY_AUTHENTICATION,
    _KEY_PWD,
    _KEY_UID,
)
from mssql_python.exceptions import InterfaceError

_SQL_PASSWORD = "sqlpassword"
_AD_PASSWORD = "activedirectorypassword"
_MANAGED_IDENTITY = "activedirectorymanagedidentity"

# Acquired by Connection.__init__ (auth.py) before the native connect.
_TOKEN_MODES = frozenset(
    {
        AuthType.INTERACTIVE.value,
        AuthType.DEVICE_CODE.value,
        AuthType.DEFAULT.value,
        AuthType.MSI.value,
    }
)
_UNSUPPORTED = {
    "activedirectoryintegrated": (
        "Authentication=ActiveDirectoryIntegrated is not supported by the Rust core. "
        "For Kerberos (GSSAPI) integrated login use Trusted_Connection=yes; for Entra ID "
        "pass a token with token_provider= or Authentication=ActiveDirectoryDefault."
    ),
    "activedirectoryworkloadidentity": (
        "Authentication=ActiveDirectoryWorkloadIdentity is not supported by the Rust core. "
        "Pass a token with token_provider= or use Authentication=ActiveDirectoryDefault."
    ),
}
_KNOWN = (
    _TOKEN_MODES
    | {AuthType.SERVICE_PRINCIPAL.value, _SQL_PASSWORD, _AD_PASSWORD, _MANAGED_IDENTITY}
    | set(_UNSUPPORTED)
)

# Public client id of the Azure CLI, the one tenant-agnostic first-party client
# that allows the username/password (ROPC) grant without an app registration.
_PASSWORD_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

_AZURE_HINT = "Install the optional dependency: pip install mssql-python-rs[azure]"


def _require_azure_identity(mode: str) -> None:
    try:
        import azure.identity  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import
    except ImportError as e:
        raise InterfaceError(
            driver_error=f"Authentication={mode} needs azure-identity. {_AZURE_HINT}",
            ddbc_error="azure-identity is not installed.",
        ) from e


def make_password_token_factory(username: str, password: str) -> Callable[[str, str, str], bytes]:
    """Token factory for ``ActiveDirectoryPassword``.

    Same contract as ``ServicePrincipalAuth.make_token_factory``: called by the
    native login with ``(spn, sts_url, auth_method)`` once the server has named
    the tenant, returns the JWT as bare UTF-16LE bytes.
    """
    if not username or not password:
        raise ValueError("ActiveDirectoryPassword auth requires UID (user principal name) and PWD")

    def _factory(spn: str, sts_url: str, _auth_method: str) -> bytes:
        # pylint: disable=import-outside-toplevel
        from azure.identity import UsernamePasswordCredential
        from azure.core.exceptions import ClientAuthenticationError
        from mssql_python.auth import _parse_tenant_id

        if not spn:
            raise RuntimeError("ActiveDirectoryPassword: empty SPN from server")
        tenant_id = _parse_tenant_id(sts_url)
        if not tenant_id:
            raise RuntimeError(f"Could not extract tenant_id from STS URL: {sts_url!r}")
        scope = spn if spn.endswith("/.default") else spn.rstrip("/") + "/.default"
        try:
            credential = UsernamePasswordCredential(
                client_id=_PASSWORD_CLIENT_ID,
                username=username,
                password=password,
                tenant_id=tenant_id,
            )
            token = credential.get_token(scope).token
        except ClientAuthenticationError:
            # The provider text can echo the account; keep it out of the exception chain.
            raise RuntimeError(
                "ActiveDirectoryPassword authentication failed (wrong credentials, MFA "
                "required, or the account is not allowed the password grant)"
            ) from None
        return token.encode("utf-16-le")

    return _factory


def prepare(parsed_params: Dict[str, str]) -> Optional[Callable[[str, str, str], bytes]]:
    """Validate ``Authentication`` and return the federated token factory, if any.

    ``ActiveDirectoryManagedIdentity`` is rewritten in *parsed_params* to its
    ``ActiveDirectoryMSI`` alias so the token path in ``Connection.__init__``
    picks it up. Raises ``InterfaceError`` for unknown or unsupported values.
    """
    raw = parsed_params.get(_KEY_AUTHENTICATION, "")
    mode = raw.strip().lower()
    if not mode:
        return None
    if mode not in _KNOWN:
        raise InterfaceError(
            driver_error=f"Invalid value specified for connection string attribute 'Authentication': {raw}",
            ddbc_error=f"Unknown Authentication value: {raw}",
        )
    if mode in _UNSUPPORTED:
        raise InterfaceError(driver_error=_UNSUPPORTED[mode], ddbc_error=f"Unsupported: {raw}")
    if mode == _MANAGED_IDENTITY:
        parsed_params[_KEY_AUTHENTICATION] = "ActiveDirectoryMSI"
        return None
    if mode not in (AuthType.SERVICE_PRINCIPAL.value, _AD_PASSWORD):
        return None
    _require_azure_identity(raw)
    uid = (parsed_params.get(_KEY_UID) or "").strip()
    pwd = parsed_params.get(_KEY_PWD) or ""
    try:
        if mode == _AD_PASSWORD:
            return make_password_token_factory(uid, pwd)
        # pylint: disable=import-outside-toplevel
        from mssql_python.auth import ServicePrincipalAuth

        return ServicePrincipalAuth.make_token_factory(uid, pwd)
    except ValueError as e:
        raise InterfaceError(
            driver_error=f"Authentication={raw} requires UID and PWD: {e}",
            ddbc_error=str(e),
        ) from e


def reject_unhandled(parsed_params: Dict[str, str]) -> None:
    """Raise when a token mode produced no token (Windows Interactive).

    Upstream leaves Windows Interactive to ODBC's native AAD prompt, which the
    Rust core does not have; connecting anyway would be a SQL login.
    """
    mode = parsed_params.get(_KEY_AUTHENTICATION, "").strip().lower()
    if mode in _TOKEN_MODES:
        raise InterfaceError(
            driver_error=(
                f"Authentication={parsed_params[_KEY_AUTHENTICATION]} is not available on "
                f"{sys.platform}: the Rust core has no native interactive flow. "
                "Use token_provider= or Authentication=ActiveDirectoryDefault."
            ),
            ddbc_error="No native interactive Entra ID flow.",
        )
