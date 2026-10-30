//! ODBC connection string -> mssql-tds `ClientContext`.
//!
//! Handles `key=value;` pairs with `{braced;values}` (`}}` escapes a brace),
//! case-insensitive keys and the common msodbcsql synonyms. Unknown keys are
//! ignored (msodbcsql does the same) so the Python layer's allow-list stays
//! the single source of validation.

use std::collections::HashMap;

use mssql_tds::connection::client_context::{ClientContext, TdsAuthenticationMethod};
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

/// Options that live outside `ClientContext`.
#[derive(Debug, Clone, Default)]
pub struct ConnOptions {
    pub login_timeout: Option<u32>,
}

pub fn build_context(
    conn_str: &str,
    access_token: Option<String>,
) -> Result<(ClientContext, ConnOptions), String> {
    let map = parse(conn_str);
    let server = first(&map, &["server", "address", "addr", "data source"])
        .unwrap_or("localhost")
        .to_string();
    let mut ctx = ClientContext::with_data_source(&server);
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

    if let Some(token) = access_token {
        ctx.tds_authentication_method = TdsAuthenticationMethod::AccessToken;
        ctx.access_token = Some(token);
        ctx.user_name.clear();
        ctx.password.clear();
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
/// followed by the token as UTF-16LE (msodbcsql's ACCESSTOKEN layout).
pub fn decode_access_token(raw: &[u8]) -> Option<String> {
    if raw.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let body = raw.get(4..4 + len.min(raw.len() - 4))?;
    let units: Vec<u16> = body
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16(&units).ok()
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
}
