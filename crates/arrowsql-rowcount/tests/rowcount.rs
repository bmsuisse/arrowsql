//! Runs the example against a live SQL Server (skipped unless
//! DB_CONNECTION_STRING is set): the single read, the merged parallel read
//! and the per-partition Arrow C streams must count the same rows.

use std::process::Command;

fn counts(output: &str) -> Vec<u64> {
    output
        .lines()
        .filter(|l| l.starts_with("rowcount"))
        .map(|l| l.rsplit(' ').next().unwrap().parse().unwrap())
        .collect()
}

#[test]
fn counts_rows_single_and_partitioned() {
    if std::env::var_os("DB_CONNECTION_STRING").is_none() {
        eprintln!("DB_CONNECTION_STRING not set: skipped");
        return;
    }
    let query = "SELECT TOP (50000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS bigint) AS id \
                 FROM sys.all_columns a CROSS JOIN sys.all_columns b";
    let run = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_arrowsql-rowcount"))
            .args(args)
            .output()
            .expect("run the example");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    let single = run(&[query]);
    assert_eq!(counts(&single), vec![50_000], "{single}");
    let split = run(&[query, "id", "4"]);
    assert_eq!(counts(&split), vec![50_000, 50_000], "{split}");
    assert!(split.contains("partition 1:"), "{split}");
}
