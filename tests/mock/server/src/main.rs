//! Drives `mssql-mock-tds` from a test process.
//!
//! The upstream CLI cannot register query responses and the upstream Python
//! bindings cannot either, so this thin binary exposes the library API over
//! JSON lines: one command per stdin line, one JSON reply per stdout line.
//! The first stdout line is `{"event":"listening","port":N}`. The server exits
//! on `quit` or when stdin closes (the parent test process died).
//!
//! Args: `--tls none|optional|strict`, `--cert F --key F` (PEM, non-Windows) or
//! `--pfx F [--pfx-password P]`, `--redirect HOST:PORT[:DATABASE]`.

use mssql_mock_tds::{
    ColumnDefinition, ColumnValue, InfoMessage, LOGIN_DELAY_KEY, LeadingError, MockTdsServer,
    QueryResponse, RPC_DELAY_KEY, RedirectionConfig, Row, SqlDataType, TM_BEGIN_DELAY_KEY,
    TerminalError,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn identity(args: &[String]) -> Res<native_tls::Identity> {
    if let Some(pfx) = arg(args, "--pfx") {
        let pw = arg(args, "--pfx-password").unwrap_or_default();
        return mssql_mock_tds::load_identity_from_file(&pfx, &pw);
    }
    let cert = arg(args, "--cert").ok_or("TLS needs --pfx or --cert/--key")?;
    let key = arg(args, "--key").ok_or("--cert needs --key")?;
    mssql_mock_tds::create_test_identity(&std::fs::read(cert)?, &std::fs::read(key)?)
}

fn column_type(name: &str) -> Res<SqlDataType> {
    Ok(match name {
        "tinyint" => SqlDataType::TinyInt,
        "smallint" => SqlDataType::SmallInt,
        "int" => SqlDataType::Int,
        "bigint" => SqlDataType::BigInt,
        "nvarchar" => SqlDataType::NVarChar,
        "nvarchar_max" => SqlDataType::NVarCharMax,
        "varchar_max" => SqlDataType::VarCharMax,
        "varbinary_max" => SqlDataType::VarBinaryMax,
        other => return Err(format!("unknown column type {other}").into()),
    })
}

fn hex(s: &str) -> Res<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err("odd hex length".into());
    }
    (0..s.len()).step_by(2).map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?)).collect()
}

fn chunks(v: &Value) -> Res<&Vec<Value>> {
    v.as_array().ok_or_else(|| "PLP value must be a list of chunks".into())
}

/// Integers are JSON numbers, nvarchar a string. PLP types are lists of chunks:
/// plain strings for nvarchar_max, hex strings for varchar_max and varbinary_max.
fn cell(ty: &SqlDataType, v: &Value) -> Res<ColumnValue> {
    if v.is_null() {
        return Ok(match ty {
            SqlDataType::NVarCharMax => ColumnValue::NVarCharMaxNull,
            _ => ColumnValue::Null,
        });
    }
    Ok(match ty {
        SqlDataType::TinyInt => ColumnValue::TinyInt(v.as_u64().ok_or("tinyint")? as u8),
        SqlDataType::SmallInt => ColumnValue::SmallInt(v.as_i64().ok_or("smallint")? as i16),
        SqlDataType::Int => ColumnValue::Int(v.as_i64().ok_or("int")? as i32),
        SqlDataType::BigInt => ColumnValue::BigInt(v.as_i64().ok_or("bigint")?),
        SqlDataType::NVarChar => ColumnValue::NVarChar(v.as_str().ok_or("nvarchar")?.to_string()),
        SqlDataType::NVarCharMax => ColumnValue::NVarCharMax(
            chunks(v)?
                .iter()
                .map(|c| Ok(c.as_str().ok_or("chunk")?.encode_utf16().collect()))
                .collect::<Res<_>>()?,
        ),
        SqlDataType::VarCharMax => ColumnValue::VarCharMax(
            chunks(v)?.iter().map(|c| hex(c.as_str().ok_or("chunk")?)).collect::<Res<_>>()?,
        ),
        SqlDataType::VarBinaryMax => ColumnValue::VarBinaryMax(
            chunks(v)?.iter().map(|c| hex(c.as_str().ok_or("chunk")?)).collect::<Res<_>>()?,
        ),
    })
}

fn info(v: &Value) -> Res<Vec<InfoMessage>> {
    let Some(list) = v.as_array() else { return Ok(vec![]) };
    list.iter()
        .map(|m| {
            Ok(InfoMessage::new(
                m["number"].as_u64().ok_or("info.number")? as u32,
                m["severity"].as_u64().unwrap_or(10) as u8,
                m["message"].as_str().ok_or("info.message")?,
            ))
        })
        .collect()
}

fn response(cmd: &Value) -> Res<(QueryResponse, Vec<InfoMessage>)> {
    let mut cols = Vec::new();
    let mut types = Vec::new();
    for c in cmd["columns"].as_array().cloned().unwrap_or_default() {
        let ty = column_type(c["type"].as_str().ok_or("column.type")?)?;
        cols.push(ColumnDefinition::new(c["name"].as_str().unwrap_or(""), ty.clone()));
        types.push(ty);
    }
    let mut rows = Vec::new();
    for r in cmd["rows"].as_array().cloned().unwrap_or_default() {
        let cells = r.as_array().ok_or("row must be a list")?;
        rows.push(Row::new(
            cells.iter().zip(&types).map(|(v, t)| cell(t, v)).collect::<Res<_>>()?,
        ));
    }
    let mut resp = if let Some(e) = cmd.get("error").filter(|e| !e.is_null()) {
        QueryResponse::error_only(TerminalError::new(
            e["number"].as_u64().ok_or("error.number")? as u32,
            e["severity"].as_u64().unwrap_or(16) as u8,
            e["message"].as_str().unwrap_or("error"),
        ))
    } else {
        QueryResponse::new(cols, rows)
    };
    resp = resp.with_info_tokens(info(&cmd["info"])?);
    if let Some(e) = cmd.get("leading_error").filter(|e| !e.is_null()) {
        resp = resp.with_leading_error(LeadingError::new(
            e["number"].as_u64().ok_or("leading_error.number")? as u32,
            e["severity"].as_u64().unwrap_or(16) as u8,
            e["message"].as_str().unwrap_or("error"),
        ));
    }
    if let Some(ms) = cmd["delay_ms"].as_u64() {
        resp = resp.with_delay(Duration::from_millis(ms));
    }
    Ok((resp, info(&cmd["trailing_info"])?))
}

fn query_key(q: &str) -> String {
    match q {
        "@login_delay" => LOGIN_DELAY_KEY.into(),
        "@rpc_delay" => RPC_DELAY_KEY.into(),
        "@tm_begin_delay" => TM_BEGIN_DELAY_KEY.into(),
        other => other.into(),
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = arg(&args, "--tls").unwrap_or_else(|| "none".into());
    let addr = "127.0.0.1:0";
    let server = if let Some(r) = arg(&args, "--redirect") {
        let parts: Vec<&str> = r.splitn(3, ':').collect();
        let (host, port) = (parts[0], parts[1].parse()?);
        let cfg = match parts.get(2) {
            Some(db) => RedirectionConfig::enhanced(host, port, *db),
            None => RedirectionConfig::new(host, port),
        };
        MockTdsServer::new_with_redirection_config(addr, cfg).await?
    } else {
        match mode.as_str() {
            "none" => MockTdsServer::new(addr).await?,
            "optional" => MockTdsServer::new_with_tls(addr, Some(identity(&args)?)).await?,
            "strict" => MockTdsServer::new_with_strict_tls(addr, identity(&args)?).await?,
            other => return Err(format!("bad --tls {other}").into()),
        }
    };
    let registry = server.query_registry();
    let store = server.connection_store();
    println!("{}", json!({"event": "listening", "port": server.local_addr().port()}));

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(server.run_with_shutdown(stop_rx));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let cmd: Value = match serde_json::from_str(&line) {
            Ok(c) => c,
            Err(e) => {
                println!("{}", json!({"ok": false, "error": e.to_string()}));
                continue;
            }
        };
        let reply = match cmd["cmd"].as_str().unwrap_or("") {
            "register" => match response(&cmd) {
                Ok((resp, trailing)) => {
                    let q = query_key(cmd["query"].as_str().unwrap_or(""));
                    let mut reg = registry.lock().await;
                    if trailing.is_empty() {
                        reg.register(q, resp);
                    } else {
                        reg.register_with_trailing_info(q, resp, trailing);
                    }
                    json!({"ok": true})
                }
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            },
            "connections" => {
                let s = store.lock().await;
                let list: Vec<Value> = s
                    .all()
                    .values()
                    .map(|c| {
                        json!({
                            "addr": c.addr.to_string(),
                            "authenticated": c.authenticated,
                            "token": c.received_token_as_string(),
                            "user_agent": c.user_agent,
                            "server_name": c.received_server_name,
                            "database": c.received_database,
                            "feature_ids": c.requested_feature_ids,
                            "fedauth_workflow": c.fedauth_workflow,
                            "tx_headers": c.transaction_descriptor_headers,
                        })
                    })
                    .collect();
                json!({"ok": true, "connections": list})
            }
            "clear_connections" => {
                store.lock().await.clear();
                json!({"ok": true})
            }
            "quit" => {
                println!("{}", json!({"ok": true}));
                break;
            }
            other => json!({"ok": false, "error": format!("unknown cmd {other}")}),
        };
        println!("{reply}");
    }
    let _ = stop_tx.send(());
    serve.abort();
    Ok(())
}
