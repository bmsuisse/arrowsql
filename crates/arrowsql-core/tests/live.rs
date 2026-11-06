//! The public API against a live SQL Server. Skipped unless
//! DB_CONNECTION_STRING is set (a database the tests may create tables in).

use std::time::{Duration, Instant};

use arrowsql_core::{
    ConnectOptions, Connection, ErrorKind, Param, Partitioning, ReadOptions, RecordBatchReader,
    read_arrow, read_arrow_stream,
};

fn options() -> Option<ConnectOptions> {
    std::env::var("DB_CONNECTION_STRING")
        .ok()
        .map(ConnectOptions::new)
}

macro_rules! connect_or_skip {
    () => {
        match options() {
            Some(options) => (Connection::connect(&options).expect("connect"), options),
            None => {
                eprintln!("DB_CONNECTION_STRING not set: skipped");
                return;
            }
        }
    };
}

/// A table of `rows` rows (`id` 1..=rows, every 7th `id` NULL in `k`), dropped on drop.
struct Table<'a> {
    conn: &'a Connection,
    name: String,
}

impl<'a> Table<'a> {
    fn create(conn: &'a Connection, tag: &str, rows: i64) -> Table<'a> {
        let name = format!("dbo.core_live_{tag}_{}", std::process::id());
        conn.execute(&format!("DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (id bigint NOT NULL, k int NULL, s nvarchar(40) NULL)"), &[])
            .expect("create table");
        conn.execute(
            &format!(
                "INSERT INTO {name} SELECT n, CASE WHEN n % 7 = 0 THEN NULL ELSE n END, CONCAT(N'row ', n) \
                 FROM (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n \
                 FROM sys.all_columns a CROSS JOIN sys.all_columns b) AS g"
            ),
            &[],
        )
        .expect("fill table");
        Table { conn, name }
    }
}

impl Drop for Table<'_> {
    fn drop(&mut self) {
        let _ = self
            .conn
            .execute(&format!("DROP TABLE IF EXISTS {}", self.name), &[]);
    }
}

#[test]
fn execute_and_fetch_batches() {
    let (conn, _) = connect_or_skip!();
    let mut stmt = conn
        .execute("SELECT name, database_id FROM sys.databases", &[])
        .unwrap();
    let schema = stmt.schema().expect("a result set");
    assert_eq!(schema.field(0).name(), "name");
    let mut rows = 0;
    while let Some(batch) = stmt.next_batch(2).unwrap() {
        assert!(batch.num_rows() <= 2);
        rows += batch.num_rows();
    }
    assert!(rows >= 4, "master, tempdb, model, msdb at least");
    assert!(!stmt.next_result().unwrap());
}

#[test]
fn parameters_and_rows_affected() {
    let (conn, _) = connect_or_skip!();
    let mut stmt = conn
        .execute(
            "SELECT ? AS a, ? AS b, ? AS c, ? AS d",
            &[
                Param::I64(5_000_000_000),
                Param::Str("ä".into()),
                Param::Null,
                Param::Bool(true),
            ],
        )
        .unwrap();
    let batch = stmt.next_batch(10).unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(format!("{:?}", batch.column(0).data_type()), "Int64");
    let err = conn.execute("SELECT ?", &[]).unwrap_err();
    assert_eq!(err.kind, ErrorKind::Database);

    let table = Table::create(&conn, "dml", 10);
    let stmt = conn
        .execute(
            &format!("UPDATE {} SET k = 0 WHERE id <= ?", table.name),
            &[Param::I64(4)],
        )
        .unwrap();
    assert_eq!(stmt.rows_affected(), 4);
}

#[test]
fn server_error_is_a_database_error() {
    let (conn, _) = connect_or_skip!();
    let err = conn
        .execute("SELECT * FROM dbo.no_such_table_core_live", &[])
        .expect_err("an error");
    assert_eq!(err.kind, ErrorKind::Database);
    assert_eq!(err.sqlstate, "42S02");
    assert_eq!(err.native, 208);
    // The connection stays usable.
    assert!(
        conn.execute("SELECT 1", &[])
            .unwrap()
            .next_batch(1)
            .unwrap()
            .is_some()
    );
}

#[test]
fn reader_on_another_thread() {
    let (conn, _) = connect_or_skip!();
    let stmt = conn
        .execute(
            "SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b",
            &[],
        )
        .unwrap();
    let reader = stmt.into_reader(8192, 2).unwrap();
    assert_eq!(reader.schema().field(0).name(), "n");
    let rows = std::thread::spawn(move || reader.map(|b| b.unwrap().num_rows()).sum::<usize>())
        .join()
        .unwrap();
    assert_eq!(rows, 100_000);
    drop(conn);
}

#[test]
fn reader_dropped_early_frees_the_connection() {
    let (conn, _) = connect_or_skip!();
    let stmt = conn
        .execute(
            "SELECT TOP (500000) a.object_id FROM sys.all_columns a CROSS JOIN sys.all_columns b",
            &[],
        )
        .unwrap();
    let mut reader = stmt.into_reader(1000, 1).unwrap();
    assert!(reader.next().unwrap().is_ok());
    drop(reader);
    assert!(
        conn.execute("SELECT 1", &[])
            .unwrap()
            .next_batch(1)
            .unwrap()
            .is_some()
    );
}

#[test]
fn cancel_a_running_statement() {
    let (conn, _) = connect_or_skip!();
    let cancel = conn.cancel_handle();
    let started = Instant::now();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        cancel.cancel();
    });
    let err = conn
        .execute("WAITFOR DELAY '00:00:20'; SELECT 1", &[])
        .expect_err("cancelled");
    canceller.join().unwrap();
    assert_eq!(err.sqlstate, "HY008");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        conn.execute("SELECT 1", &[])
            .unwrap()
            .next_batch(1)
            .unwrap()
            .is_some()
    );
}

#[test]
fn partitioned_read_matches_single_read() {
    let (conn, options) = connect_or_skip!();
    let table = Table::create(&conn, "par", 60_000);
    let query = format!("SELECT id, k, s FROM {}", table.name);
    let sorted_ids = |batches: Vec<arrowsql_core::RecordBatch>| {
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                col.values().to_vec()
            })
            .collect();
        ids.sort_unstable();
        ids
    };
    let (_, single) =
        read_arrow(&options, &query, Partitioning::None, ReadOptions::default()).unwrap();
    for partitioning in [
        Partitioning::Range {
            column: "k".into(),
            partitions: 4,
        },
        Partitioning::Bounds {
            column: "id".into(),
            bounds: vec![Param::I64(10_000), Param::I64(30_000)],
            nullable: None,
            partitions: 3,
        },
    ] {
        let reader = read_arrow_stream(
            &options,
            &query,
            partitioning.clone(),
            ReadOptions::default(),
        )
        .unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(
            sorted_ids(batches),
            sorted_ids(single.clone()),
            "{partitioning:?}"
        );
    }
    let (_, table_read) = read_arrow(
        &options,
        &query,
        Partitioning::Range {
            column: "id".into(),
            partitions: 3,
        },
        ReadOptions::default(),
    )
    .unwrap();
    assert_eq!(sorted_ids(table_read).len(), 60_000);
}

#[test]
fn partitioned_read_errors() {
    let (conn, options) = connect_or_skip!();
    let table = Table::create(&conn, "err", 30_000);
    let err = read_arrow_stream(
        &options,
        &format!("SELECT * FROM {}", table.name),
        Partitioning::Range {
            column: "s".into(),
            partitions: 4,
        },
        ReadOptions::default(),
    )
    .err()
    .expect("not an integer column");
    assert_eq!(err.kind, ErrorKind::Type);
    // A slice fails mid-read: the stream ends with the error.
    let mut reader = read_arrow_stream(
        &options,
        &format!("SELECT id, 1 / (id - 29000) AS x FROM {}", table.name),
        Partitioning::Range {
            column: "id".into(),
            partitions: 4,
        },
        ReadOptions::default(),
    )
    .unwrap();
    // An error while reading a slice ends the stream with its text.
    let message = reader
        .by_ref()
        .find_map(|b| b.err())
        .expect("the read fails")
        .to_string();
    assert!(message.contains("[22012]"), "{message}");
    reader.close();
}

#[test]
fn closing_a_partitioned_read_early() {
    let (conn, options) = connect_or_skip!();
    let table = Table::create(&conn, "close", 50_000);
    let mut reader = read_arrow_stream(
        &options,
        &format!("SELECT id, s FROM {}", table.name),
        Partitioning::Range {
            column: "id".into(),
            partitions: 4,
        },
        ReadOptions {
            batch_rows: 500,
            max_buffered: Some(2),
            utf8_strings: false,
        },
    )
    .unwrap();
    assert!(reader.next().unwrap().is_ok());
    let started = Instant::now();
    reader.close();
    assert!(reader.is_closed());
    assert!(started.elapsed() < Duration::from_secs(10));
}
