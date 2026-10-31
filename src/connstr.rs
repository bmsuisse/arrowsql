//! ODBC connection string -> mssql-tds `ClientContext`.
//!
//! Handles `key=value;` pairs with `{braced;values}` (`}}` escapes a brace),
//! case-insensitive keys and the common msodbcsql synonyms. Unknown keys are
//! ignored (msodbcsql does the same) so the Python layer's allow-list stays
//! the single source of validation.

use std::collections::HashMap;

use mssql_tds::connection::client_context::{
    CloneableEntraIdTokenFactory, ClientContext, IPAddressPreference, TdsAuthenticationMethod,
};
use mssql_tds::core::EncryptionSetting;
use mssql_tds::message::login_options::ApplicationIntent;

/// Parse into lowercase-key -> raw value. Later keys win, like ODBC.
pub fn parse(conn_str: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let chars: Vec<char> = conn_str.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && (chars[i] == ';' || chars[i].is_whitespace()) {
            i += 1;
        }
        let key_start = i;
        while i < chars.len() && chars[i] != '=' && chars[i] != ';' {
            i += 1;
        }
        let key: String = chars[key_start..i].iter().collect::<String>().trim().to_ascii_lowercase();
        if i >= chars.len() || chars[i] == ';' {
            continue;
        }
        i += 1; // '='
        while i < chars.len() && chars[i] == ' ' {
            i += 1;
        }
        let mut value = String::new();
        if i < chars.len() && chars[i] == '{' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '}' {
                    if i + 1 < chars.len() && chars[i + 1] == '}' {
                        value.push('}');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                value.push(chars[i]);
                i += 1;
            }
            while i < chars.len() && chars[i] != ';' {
                i += 1;
            }
        } else {
            let start = i;
            while i < chars.len() && chars[i] != ';' {
                i += 1;
            }
            value = chars[start..i].iter().collect::<String>().trim().to_string();
        }
        if !key.is_empty() {
            out.insert(key, value);
        }
    }
    out
}

fn first<'a>(map: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| map.get(*k).map(String::as_str))
}

fn yes(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "yes" | "true" | "1")
}

/// TDS packet size requested when the connection string does not set one.
pub const DEFAULT_PACKET_SIZE: u16 = 16384;

/// Options that live outside `ClientContext`.
#[derive(Debug, Clone, Default)]
pub struct ConnOptions {
    pub login_timeout: Option<u32>,
}

/// Error text for a connection string without a server keyword.
pub const MISSING_SERVER: &str = "Neither DSN nor SERVER keyword supplied";

pub fn build_context(
    conn_str: &str,
    access_token: Option<String>,
) -> Result<(ClientContext, ConnOptions), String> {
    build_context_with(conn_str, access_token, None)
}

/// What an `Authentication=` value asks of the driver.
#[derive(Debug, PartialEq, Eq)]
enum AuthKeyword {
    /// Absent, empty or `SqlPassword`: SQL login (or SSPI/Kerberos when
    /// `Trusted_Connection` says so).
    Sql,
    /// Entra ID flow whose token factory is told the tenant by the server
    /// during login (`ActiveDirectoryPassword` / `ActiveDirectoryServicePrincipal`).
    Federated(TdsAuthenticationMethod),
    /// A mode that needs a token the caller acquires up front.
    Token(&'static str),
    /// Known to ODBC, not available here.
    Unsupported(&'static str),
}

fn auth_keyword(value: &str) -> Result<AuthKeyword, String> {
    Ok(match value.trim().to_ascii_lowercase().as_str() {
        "" | "sqlpassword" => AuthKeyword::Sql,
        "activedirectorypassword" => AuthKeyword::Federated(TdsAuthenticationMethod::ActiveDirectoryPassword),
        "activedirectoryserviceprincipal" => {
            AuthKeyword::Federated(TdsAuthenticationMethod::ActiveDirectoryServicePrincipal)
        }
        "activedirectoryinteractive" => AuthKeyword::Token("ActiveDirectoryInteractive"),
        "activedirectorydevicecode" => AuthKeyword::Token("ActiveDirectoryDeviceCode"),
        "activedirectorydefault" => AuthKeyword::Token("ActiveDirectoryDefault"),
        "activedirectorymsi" => AuthKeyword::Token("ActiveDirectoryMSI"),
        "activedirectorymanagedidentity" => AuthKeyword::Token("ActiveDirectoryManagedIdentity"),
        "activedirectoryintegrated" => AuthKeyword::Unsupported(
            "Authentication=ActiveDirectoryIntegrated is not supported; for Kerberos/GSSAPI \
             integrated login use Trusted_Connection=yes, or pass a token via token_provider=",
        ),
        "activedirectoryworkloadidentity" => AuthKeyword::Unsupported(
            "Authentication=ActiveDirectoryWorkloadIdentity is not supported; \
             pass a token via token_provider=",
        ),
        _ => {
            return Err(format!(
                "Invalid value specified for connection string attribute 'Authentication': {value}"
            ));
        }
    })
}

/// A positive integer keyword (ODBC rejects 0 and non-numeric values).
fn positive_u32(map: &HashMap<String, String>, key: &str, label: &str) -> Result<Option<u32>, String> {
    let Some(raw) = map.get(key) else { return Ok(None) };
    match raw.trim().parse::<u32>() {
        Ok(n) if n > 0 => Ok(Some(n)),
        _ => Err(format!("Invalid value specified for connection string attribute '{label}': {raw}")),
    }
}

/// Like [`build_context`]; `federated` is the token factory used for the
/// `ActiveDirectoryPassword` / `ActiveDirectoryServicePrincipal` flows. Every
/// `Authentication=` value either maps to a login method or is an error: it
/// never degrades to a SQL login.
pub fn build_context_with(
    conn_str: &str,
    access_token: Option<String>,
    federated: Option<Box<dyn CloneableEntraIdTokenFactory>>,
) -> Result<(ClientContext, ConnOptions), String> {
    let map = parse(conn_str);
    let server = first(&map, &["server", "address", "addr", "data source"])
        .ok_or_else(|| MISSING_SERVER.to_string())?
        .to_string();
    let mut ctx = ClientContext::with_data_source(&server);
    // A session the server ended (KILL, network loss) is reported to the
    // caller, not reconnected behind its back: upstream does the same.
    // Session recovery is only requested when ConnectRetryCount says so.
    ctx.connect_retry_count = 0;
    let mut opts = ConnOptions::default();

    if let Some(uid) = first(&map, &["uid", "user id", "user"]) {
        ctx.user_name = uid.to_string();
    }
    if let Some(pwd) = first(&map, &["pwd", "password"]) {
        ctx.password = pwd.to_string();
    }
    if let Some(db) = first(&map, &["database", "initial catalog"]) {
        ctx.database = db.to_string();
    }
    ctx.application_name = first(&map, &["app", "application name"])
        .unwrap_or("MSSQL-Python")
        .to_string();
    ctx.library_name = "MSSQL-Python".to_string();
    if let Some(wsid) = first(&map, &["wsid", "workstation id"]) {
        ctx.workstation_id = wsid.to_string();
    }
    if let Some(lang) = first(&map, &["language"]) {
        ctx.language = lang.to_string();
    }

    let encrypt = first(&map, &["encrypt"]).unwrap_or("yes");
    ctx.encryption_options.mode = match encrypt.to_ascii_lowercase().as_str() {
        "yes" | "true" | "mandatory" | "required" => EncryptionSetting::Required,
        "no" | "false" | "optional" | "disabled" => EncryptionSetting::PreferOff,
        "strict" => EncryptionSetting::Strict,
        other => return Err(format!("Invalid value specified for connection string attribute 'Encrypt': {other}")),
    };
    ctx.encryption_options.trust_server_certificate =
        first(&map, &["trustservercertificate", "trust server certificate"]).is_some_and(yes);
    if let Some(host) = first(&map, &["hostnameincertificate", "host name in certificate"]) {
        ctx.encryption_options.host_name_in_cert = Some(host.to_string());
    }
    if let Some(path) = first(&map, &["servercertificate"]) {
        ctx.encryption_options.server_certificate = Some(path.into());
    }

    if let Some(t) = first(&map, &["connection timeout", "connect timeout", "logintimeout", "timeout"])
        && let Ok(t) = t.parse::<u32>()
    {
        ctx.connect_timeout = t;
        opts.login_timeout = Some(t);
    }
    // 16384 measured 10-15% faster than the 8000 default on large reads
    // (docs/PERFORMANCE_RESEARCH.md §2); an explicit PacketSize still wins.
    ctx.packet_size = DEFAULT_PACKET_SIZE;
    if let Some(p) = first(&map, &["packet size", "packetsize"])
        && let Ok(p) = p.parse::<u16>()
    {
        ctx.packet_size = p;
    }
    if let Some(intent) = first(&map, &["applicationintent", "application intent"]) {
        ctx.application_intent = if intent.eq_ignore_ascii_case("readonly") {
            ApplicationIntent::ReadOnly
        } else {
            ApplicationIntent::ReadWrite
        };
    }
    if let Some(msf) = first(&map, &["multisubnetfailover", "multi subnet failover"]) {
        ctx.multi_subnet_failover = Some(yes(msf));
    }
    if let Some(n) = first(&map, &["connectretrycount"]).and_then(|v| v.parse().ok()) {
        ctx.connect_retry_count = n;
    }
    if let Some(n) = first(&map, &["connectretryinterval"]).and_then(|v| v.parse().ok()) {
        ctx.connect_retry_interval = n;
    }

    if let Some(spn) = first(&map, &["serverspn"]).filter(|v| !v.is_empty()) {
        ctx.server_spn = Some(spn.to_string());
    }
    // ODBC: both are positive millisecond counts. TCP_KEEPIDLE/TCP_KEEPINTVL
    // take whole seconds (a sub-second value truncates to 0 and the socket
    // setup fails with EINVAL), so round up to the next second.
    let round_up_secs = |ms: u32| ms.div_ceil(1000).saturating_mul(1000);
    if let Some(n) = positive_u32(&map, "keepalive", "KeepAlive")? {
        ctx.keep_alive_in_ms = round_up_secs(n);
    }
    if let Some(n) = positive_u32(&map, "keepaliveinterval", "KeepAliveInterval")? {
        ctx.keep_alive_interval_in_ms = round_up_secs(n);
    }
    if let Some(pref) = first(&map, &["ipaddresspreference"]) {
        ctx.ipaddress_preference = match pref.to_ascii_lowercase().as_str() {
            "ipv4first" => IPAddressPreference::IPv4First,
            "ipv6first" => IPAddressPreference::IPv6First,
            "useplatformdefault" => {
                IPAddressPreference::UsePlatformDefault
            }
            _ => {
                return Err(format!(
                    "Invalid value specified for connection string attribute 'IpAddressPreference': {pref}"
                ));
            }
        };
    }

    let keyword = auth_keyword(first(&map, &["authentication"]).unwrap_or(""))?;
    if let Some(token) = access_token {
        ctx.tds_authentication_method = TdsAuthenticationMethod::AccessToken;
        ctx.access_token = Some(token);
        ctx.user_name.clear();
        ctx.password.clear();
    } else if let AuthKeyword::Federated(method) = keyword {
        let factory = federated.ok_or_else(|| {
            format!(
                "Authentication={} needs a token factory; connect through mssql_python.connect \
                 (requires azure-identity: pip install mssql-python-rs[azure])",
                first(&map, &["authentication"]).unwrap_or_default()
            )
        })?;
        // The identity travels in the token, never in LOGIN7.
        ctx.user_name.clear();
        ctx.password.clear();
        ctx.auth_method_map.insert(method.clone(), factory);
        ctx.tds_authentication_method = method;
    } else if let AuthKeyword::Token(name) = keyword {
        return Err(format!(
            "Authentication={name} needs an access token acquired before connecting \
             (requires azure-identity: pip install mssql-python-rs[azure])"
        ));
    } else if let AuthKeyword::Unsupported(why) = keyword {
        return Err(why.to_string());
    } else if first(&map, &["trusted_connection", "integrated security"]).is_some_and(|v| {
        yes(v) || v.eq_ignore_ascii_case("sspi")
    }) {
        ctx.tds_authentication_method = TdsAuthenticationMethod::SSPI;
    } else {
        ctx.tds_authentication_method = TdsAuthenticationMethod::Password;
    }
    Ok((ctx, opts))
}

/// Decode an `SQL_COPT_SS_ACCESS_TOKEN` struct: little-endian u32 byte length
/// followed by exactly that many bytes of UTF-16LE token (msodbcsql's
/// ACCESSTOKEN layout). Anything else is rejected rather than ignored, so a
/// malformed token can never fall back to another login method.
pub fn decode_access_token(raw: &[u8]) -> Result<String, String> {
    let malformed = |why: &str| format!("Invalid access token: {why}");
    let Some((prefix, body)) = raw.split_first_chunk::<4>() else {
        return Err(malformed("shorter than its 4-byte length prefix"));
    };
    let len = u32::from_le_bytes(*prefix) as usize;
    if len != body.len() {
        return Err(malformed("length prefix does not match the token size"));
    }
    if len == 0 || !len.is_multiple_of(2) {
        return Err(malformed("token must be a non-empty UTF-16LE string"));
    }
    let units: Vec<u16> = body.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect();
    String::from_utf16(&units).map_err(|_| malformed("token is not valid UTF-16"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_braces_and_case() {
        let m = parse("Server=tcp:h,1433;PWD={a;b}}c};Database = db ;");
        assert_eq!(m["server"], "tcp:h,1433");
        assert_eq!(m["pwd"], "a;b}c");
        assert_eq!(m["database"], "db");
    }

    #[test]
    fn packet_size_defaults_to_16384_and_honours_explicit_value() {
        let (ctx, _) = build_context("Server=h;", None).unwrap();
        assert_eq!(ctx.packet_size, DEFAULT_PACKET_SIZE);
        let (ctx, _) = build_context("Server=h;Packet Size=4096;", None).unwrap();
        assert_eq!(ctx.packet_size, 4096);
    }

    #[test]
    fn authentication_never_degrades_to_sql_login() {
        for bad in ["Authentication=Bogus", "Authentication=ActiveDirectoryIntegrated", "Authentication=ActiveDirectoryInteractive", "Authentication=ActiveDirectoryServicePrincipal;UID=a;PWD=b"] {
            assert!(build_context(&format!("Server=h;{bad}"), None).is_err(), "{bad}");
        }
        let (ctx, _) = build_context("Server=h;Authentication=SqlPassword;UID=u;PWD=p", None).unwrap();
        assert_eq!(ctx.tds_authentication_method, TdsAuthenticationMethod::Password);
        assert_eq!(ctx.user_name, "u");
        let (ctx, _) = build_context("Server=h;Authentication=ActiveDirectoryMSI;UID=u", Some("t".into())).unwrap();
        assert_eq!(ctx.tds_authentication_method, TdsAuthenticationMethod::AccessToken);
    }

    #[test]
    fn network_keywords_are_applied_and_validated() {
        let (ctx, _) = build_context(
            "Server=h;ServerSPN=MSSQLSvc/h:1433;KeepAlive=5000;KeepAliveInterval=250;IpAddressPreference=IPv6First",
            None,
        )
        .unwrap();
        assert_eq!(ctx.server_spn.as_deref(), Some("MSSQLSvc/h:1433"));
        assert_eq!(ctx.keep_alive_in_ms, 5000);
        assert_eq!(ctx.keep_alive_interval_in_ms, 1000, "rounded up to whole seconds");
        assert!(ctx.ipaddress_preference == IPAddressPreference::IPv6First);
        for bad in ["KeepAlive=0", "KeepAlive=x", "KeepAliveInterval=-1", "IpAddressPreference=none"] {
            assert!(build_context(&format!("Server=h;{bad}"), None).is_err(), "{bad}");
        }
    }

    #[test]
    fn access_token_must_match_its_length_prefix() {
        let mut raw = 6u32.to_le_bytes().to_vec();
        raw.extend("abc".encode_utf16().flat_map(u16::to_le_bytes));
        assert_eq!(decode_access_token(&raw).as_deref(), Ok("abc"));
        assert!(decode_access_token(b"x").is_err());
        raw.push(0);
        assert!(decode_access_token(&raw).is_err());
    }
}
