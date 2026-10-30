//! Speed-of-light probe over `mssql-tds`: how fast can a pure-Rust TDS client
//! read SQL Server data with no Python in the loop?
//!
//!   tds_probe read  [--sink noop|columnar|values] [--packet N] [--encrypt off|on|strict] [--query Q] [--repeat K]
//!   tds_probe parts [--n N] [--sink ...] [--packet N] [--encrypt ...]   (PK-range partitioned read of yellow_trips)
//!   tds_probe latency [--n N] [--encrypt ...]                         (SELECT 1 / point lookup round trips)
//!
//! Connection: DB_HOST (default localhost), DB_PORT (1433), DB_USER (sa),
//! MSSQL_SA_PASSWORD, DB_NAME (nyctaxi). Prints one JSON object per run.

use std::borrow::Cow;
use std::time::Instant;

use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::connection::tds_client::{ResultSet, TdsClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::{EncryptionOptions, EncryptionSetting};
use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::EncodingType;
use mssql_tds::datatypes::sql_vector::SqlVector;
use uuid::Uuid;

const DEFAULT_QUERY: &str = "SELECT * FROM dbo.yellow_trips";

struct Args {
    cmd: String,
    sink: String,
    packet: u16,
    encrypt: String,
    query: String,
    repeat: usize,
    n: usize,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        cmd: it.next().unwrap_or_else(|| "read".into()),
        sink: "noop".into(),
        packet: 8000,
        encrypt: "on".into(),
        query: DEFAULT_QUERY.into(),
        repeat: 3,
        n: 4,
    };
    while let Some(flag) = it.next() {
        let v = it.next().expect("flag needs a value");
        match flag.as_str() {
            "--sink" => a.sink = v,
            "--packet" => a.packet = v.parse().unwrap(),
            "--encrypt" => a.encrypt = v,
            "--query" => a.query = v,
            "--repeat" => a.repeat = v.parse().unwrap(),
            "--n" => a.n = v.parse().unwrap(),
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

fn context(packet: u16, encrypt: &str) -> ClientContext {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let mut ctx = ClientContext::default();
    ctx.user_name = env("DB_USER", "sa");
    ctx.password = std::env::var("MSSQL_SA_PASSWORD").expect("MSSQL_SA_PASSWORD");
    ctx.database = env("DB_NAME", "nyctaxi");
    ctx.packet_size = packet;
    ctx.encryption_options = EncryptionOptions {
        mode: match encrypt {
            "off" => EncryptionSetting::PreferOff,
            "strict" => EncryptionSetting::Strict,
            _ => EncryptionSetting::On,
        },
        trust_server_certificate: true,
        host_name_in_cert: None,
        server_certificate: None,
    };
    ctx
}

async fn connect(packet: u16, encrypt: &str) -> TdsClient {
    let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("DB_PORT").unwrap_or_else(|_| "1433".into());
    TdsConnectionProvider {}
        .create_client(context(packet, encrypt), &format!("tcp:{host},{port}"), None)
        .await
        .expect("connect")
}

/// Discards every value: measures TDS framing + token parsing only.
#[derive(Default)]
struct NoopSink {
    rows: u64,
}

/// Simulates Arrow builders: one typed buffer per column, strings transcoded
/// to UTF-8 into a shared values buffer with an offsets vector.
#[derive(Default)]
struct ColumnarSink {
    rows: u64,
    i64s: Vec<Vec<i64>>,
    f64s: Vec<Vec<f64>>,
    i128s: Vec<Vec<i128>>,
    utf8: Vec<(Vec<u8>, Vec<i32>)>,
    valid: Vec<Vec<bool>>,
}

impl ColumnarSink {
    fn ensure(&mut self, col: usize) {
        while self.i64s.len() <= col {
            self.i64s.push(Vec::with_capacity(65536));
            self.f64s.push(Vec::new());
            self.i128s.push(Vec::new());
            self.utf8.push((Vec::new(), vec![0]));
            self.valid.push(Vec::with_capacity(65536));
        }
    }
    fn int(&mut self, col: usize, v: i64) {
        self.ensure(col);
        self.i64s[col].push(v);
        self.valid[col].push(true);
    }
    fn trim(&mut self) {
        // Keep memory bounded like a batched reader (65,536-row batches).
        if self.rows % 65536 == 0 {
            for c in 0..self.i64s.len() {
                self.i64s[c].clear();
                self.f64s[c].clear();
                self.i128s[c].clear();
                self.utf8[c].0.clear();
                self.utf8[c].1.truncate(1);
                self.valid[c].clear();
            }
        }
    }
}

macro_rules! noop_writer {
    ($t:ty) => {
        impl RowWriter for $t {
            fn write_null(&mut self, _: usize) {}
            fn write_bool(&mut self, _: usize, _: bool) {}
            fn write_u8(&mut self, _: usize, _: u8) {}
            fn write_i16(&mut self, _: usize, _: i16) {}
            fn write_i32(&mut self, _: usize, _: i32) {}
            fn write_i64(&mut self, _: usize, _: i64) {}
            fn write_f32(&mut self, _: usize, _: f32) {}
            fn write_f64(&mut self, _: usize, _: f64) {}
            fn write_string(&mut self, _: usize, _: Cow<'_, [u8]>, _: EncodingType) {}
            fn write_bytes(&mut self, _: usize, _: Cow<'_, [u8]>) {}
            fn write_decimal(&mut self, _: usize, _: DecimalParts) {}
            fn write_numeric(&mut self, _: usize, _: DecimalParts) {}
            fn write_date(&mut self, _: usize, _: SqlDate) {}
            fn write_time(&mut self, _: usize, _: SqlTime) {}
            fn write_datetime(&mut self, _: usize, _: SqlDateTime) {}
            fn write_smalldatetime(&mut self, _: usize, _: SqlSmallDateTime) {}
            fn write_datetime2(&mut self, _: usize, _: SqlDateTime2) {}
            fn write_datetimeoffset(&mut self, _: usize, _: SqlDateTimeOffset) {}
            fn write_money(&mut self, _: usize, _: SqlMoney) {}
            fn write_smallmoney(&mut self, _: usize, _: SqlSmallMoney) {}
            fn write_uuid(&mut self, _: usize, _: Uuid) {}
            fn write_xml(&mut self, _: usize, _: SqlXml) {}
            fn write_json(&mut self, _: usize, _: SqlJson) {}
            fn write_vector(&mut self, _: usize, _: SqlVector) {}
            fn end_row(&mut self) {
                self.rows += 1;
            }
        }
    };
}
noop_writer!(NoopSink);

impl RowWriter for ColumnarSink {
    fn write_null(&mut self, col: usize) {
        self.ensure(col);
        self.valid[col].push(false);
    }
    fn write_bool(&mut self, col: usize, v: bool) {
        self.int(col, v as i64)
    }
    fn write_u8(&mut self, col: usize, v: u8) {
        self.int(col, v as i64)
    }
    fn write_i16(&mut self, col: usize, v: i16) {
        self.int(col, v as i64)
    }
    fn write_i32(&mut self, col: usize, v: i32) {
        self.int(col, v as i64)
    }
    fn write_i64(&mut self, col: usize, v: i64) {
        self.int(col, v)
    }
    fn write_f32(&mut self, col: usize, v: f32) {
        self.ensure(col);
        self.f64s[col].push(v as f64);
        self.valid[col].push(true);
    }
    fn write_f64(&mut self, col: usize, v: f64) {
        self.ensure(col);
        self.f64s[col].push(v);
        self.valid[col].push(true);
    }
    fn write_string(&mut self, col: usize, bytes: Cow<'_, [u8]>, enc: EncodingType) {
        self.ensure(col);
        let (buf, offsets) = &mut self.utf8[col];
        match enc {
            EncodingType::Utf16 => {
                let units = bytes.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]]));
                for ch in char::decode_utf16(units) {
                    let ch = ch.unwrap_or('\u{FFFD}');
                    let mut tmp = [0u8; 4];
                    buf.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                }
            }
            _ => buf.extend_from_slice(&bytes),
        }
        offsets.push(buf.len() as i32);
        self.valid[col].push(true);
    }
    fn write_bytes(&mut self, col: usize, bytes: Cow<'_, [u8]>) {
        self.ensure(col);
        let (buf, offsets) = &mut self.utf8[col];
        buf.extend_from_slice(&bytes);
        offsets.push(buf.len() as i32);
        self.valid[col].push(true);
    }
    fn write_decimal(&mut self, col: usize, v: DecimalParts) {
        self.ensure(col);
        let m = v.magnitude() as i128;
        self.i128s[col].push(if v.is_positive { m } else { -m });
        self.valid[col].push(true);
    }
    fn write_numeric(&mut self, col: usize, v: DecimalParts) {
        self.write_decimal(col, v)
    }
    fn write_date(&mut self, col: usize, v: SqlDate) {
        self.int(col, v.get_days() as i64)
    }
    fn write_time(&mut self, col: usize, v: SqlTime) {
        self.int(col, v.time_nanoseconds as i64)
    }
    fn write_datetime(&mut self, col: usize, v: SqlDateTime) {
        self.int(col, ((v.days as i64) << 32) | v.time as i64)
    }
    fn write_smalldatetime(&mut self, col: usize, v: SqlSmallDateTime) {
        self.int(col, ((v.days as i64) << 16) | v.time as i64)
    }
    fn write_datetime2(&mut self, col: usize, v: SqlDateTime2) {
        // Arrow timestamp[us] since 1970-01-01 (719162 days after 0001-01-01).
        let us = (v.days as i64 - 719_162) * 86_400_000_000 + (v.time.time_nanoseconds / 1000) as i64;
        self.int(col, us)
    }
    fn write_datetimeoffset(&mut self, col: usize, v: SqlDateTimeOffset) {
        self.write_datetime2(col, v.datetime2)
    }
    fn write_money(&mut self, col: usize, v: SqlMoney) {
        self.int(col, ((v.msb_part as i64) << 32) | (v.lsb_part as u32 as i64))
    }
    fn write_smallmoney(&mut self, col: usize, v: SqlSmallMoney) {
        self.int(col, v.int_val as i64)
    }
    fn write_uuid(&mut self, col: usize, v: Uuid) {
        self.write_bytes(col, Cow::Borrowed(v.as_bytes()))
    }
    fn write_xml(&mut self, col: usize, _: SqlXml) {
        self.write_null(col)
    }
    fn write_json(&mut self, col: usize, _: SqlJson) {
        self.write_null(col)
    }
    fn write_vector(&mut self, col: usize, _: SqlVector) {
        self.write_null(col)
    }
    fn end_row(&mut self) {
        self.rows += 1;
        self.trim();
    }
}

fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
    t(ru.ru_utime) + t(ru.ru_stime)
}

async fn read_all(client: &mut TdsClient, query: &str, sink: &str) -> u64 {
    client.execute(query.to_string(), ()).await.expect("execute");
    let rows = match sink {
        "values" => {
            let mut n = 0u64;
            while client.next_row().await.expect("next_row").is_some() {
                n += 1;
            }
            n
        }
        "columnar" => {
            let mut w = ColumnarSink::default();
            while client.next_row_into(&mut w).await.expect("row") {}
            w.rows
        }
        _ => {
            let mut w = NoopSink::default();
            while client.next_row_into(&mut w).await.expect("row") {}
            w.rows
        }
    };
    client.close_query().await.expect("close_query");
    rows
}

fn report(kind: &str, a: &Args, extra: &str, rows: u64, wall: f64, cpu: f64) {
    println!(
        "{{\"cmd\":\"{kind}\",\"sink\":\"{}\",\"packet\":{},\"encrypt\":\"{}\",{extra}\"rows\":{rows},\"wall_s\":{wall:.4},\"cpu_s\":{cpu:.4},\"rows_per_s\":{:.0}}}",
        a.sink,
        a.packet,
        a.encrypt,
        rows as f64 / wall
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let a = parse_args();
    match a.cmd.as_str() {
        "read" => {
            let mut client = connect(a.packet, &a.encrypt).await;
            read_all(&mut client, &a.query, &a.sink).await; // warm-up
            for _ in 0..a.repeat {
                let (c0, t0) = (cpu_seconds(), Instant::now());
                let rows = read_all(&mut client, &a.query, &a.sink).await;
                report("read", &a, "", rows, t0.elapsed().as_secs_f64(), cpu_seconds() - c0);
            }
        }
        "parts" => {
            let mut probe = connect(a.packet, &a.encrypt).await;
            probe
                .execute("SELECT MIN(trip_id), MAX(trip_id) FROM dbo.yellow_trips".into(), ())
                .await
                .unwrap();
            let row = probe.next_row().await.unwrap().unwrap();
            let bound = |v: &mssql_tds::datatypes::column_values::ColumnValues| match v {
                mssql_tds::datatypes::column_values::ColumnValues::BigInt(x) => *x,
                other => panic!("{other:?}"),
            };
            let (lo, hi) = (bound(&row[0]), bound(&row[1]));
            probe.close_query().await.unwrap();
            for _ in 0..a.repeat {
                let mut clients = Vec::new();
                for _ in 0..a.n {
                    clients.push(connect(a.packet, &a.encrypt).await);
                }
                let span = (hi - lo + 1 + a.n as i64 - 1) / a.n as i64;
                let (c0, t0) = (cpu_seconds(), Instant::now());
                let mut tasks = Vec::new();
                for (i, mut client) in clients.into_iter().enumerate() {
                    let start = lo + i as i64 * span;
                    let q = format!(
                        "SELECT * FROM dbo.yellow_trips WHERE trip_id >= {start} AND trip_id < {}",
                        start + span
                    );
                    let sink = a.sink.clone();
                    tasks.push(tokio::spawn(async move { read_all(&mut client, &q, &sink).await }));
                }
                let mut rows = 0;
                for t in tasks {
                    rows += t.await.unwrap();
                }
                report(
                    "parts",
                    &a,
                    &format!("\"partitions\":{},", a.n),
                    rows,
                    t0.elapsed().as_secs_f64(),
                    cpu_seconds() - c0,
                );
            }
        }
        "latency" => {
            let mut client = connect(a.packet, &a.encrypt).await;
            for (label, q) in [
                ("select1", "SELECT 1".to_string()),
                ("point", "SELECT * FROM dbo.yellow_trips WHERE trip_id = 12345".to_string()),
            ] {
                read_all(&mut client, &q, "values").await;
                let t0 = Instant::now();
                for _ in 0..a.n {
                    read_all(&mut client, &q, "values").await;
                }
                let us = t0.elapsed().as_secs_f64() * 1e6 / a.n as f64;
                println!("{{\"cmd\":\"latency\",\"query\":\"{label}\",\"encrypt\":\"{}\",\"us_per_query\":{us:.1}}}", a.encrypt);
            }
            let t0 = Instant::now();
            for _ in 0..20 {
                drop(connect(a.packet, &a.encrypt).await);
            }
            println!(
                "{{\"cmd\":\"latency\",\"query\":\"connect\",\"encrypt\":\"{}\",\"ms_per_connect\":{:.2}}}",
                a.encrypt,
                t0.elapsed().as_secs_f64() * 1e3 / 20.0
            );
        }
        other => panic!("unknown command {other}"),
    }
}
