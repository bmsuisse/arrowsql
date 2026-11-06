//! Count the rows of a query with `arrowsql-core`, from plain Rust.
//!
//! ```text
//! DB_CONNECTION_STRING="Server=...;Uid=...;Pwd=..." \
//!     cargo run -p arrowsql-rowcount -- "SELECT * FROM dbo.t" [column partitions]
//! ```
//!
//! Without a column the query is read over one connection. With one, the
//! read is split on that integer column: once through the merged parallel
//! reader, once partition by partition through Arrow C streams (what a
//! Spark or pyarrow consumer would be handed).

use std::process::ExitCode;

use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrowsql_core::{
    ArrowError, ConnectOptions, Error, Partitioning, ReadOptions, RecordBatch, plan_partitions,
    read_arrow_stream,
};

// The core declares no allocator, so a dependent can pick its own.
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;

fn count(reader: impl Iterator<Item = Result<RecordBatch, ArrowError>>) -> Result<usize, String> {
    reader
        .map(|batch| batch.map(|b| b.num_rows()).map_err(|e| e.to_string()))
        .sum()
}

fn run(args: &[String]) -> Result<(), String> {
    let conn_str = std::env::var("DB_CONNECTION_STRING")
        .map_err(|_| "set DB_CONNECTION_STRING".to_string())?;
    let options = ConnectOptions::new(conn_str);
    let query = args
        .first()
        .map_or("SELECT name FROM sys.all_objects", String::as_str);
    let partitioning = match args.get(1) {
        None => Partitioning::None,
        Some(column) => Partitioning::Range {
            column: column.clone(),
            partitions: args.get(2).and_then(|p| p.parse().ok()).unwrap_or(4),
        },
    };
    let db_err = |e: Error| e.to_string();

    let reader = read_arrow_stream(
        &options,
        query,
        partitioning.clone(),
        ReadOptions::default(),
    )
    .map_err(db_err)?;
    println!("rowcount: {}", count(reader)?);

    if !matches!(partitioning, Partitioning::None) {
        let slices = plan_partitions(&options, query, &partitioning, true).map_err(db_err)?;
        let mut total = 0;
        for (i, slice) in slices.iter().enumerate() {
            let stream = slice.open_ffi_stream(&options, 65_536, 2).map_err(db_err)?;
            let reader = RecordBatchStreamReader::try_new(stream).map_err(|e| e.to_string())?;
            let rows = count(reader)?;
            println!("partition {i}: {rows} rows");
            total += rows;
        }
        println!("rowcount over {} partition streams: {total}", slices.len());
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
