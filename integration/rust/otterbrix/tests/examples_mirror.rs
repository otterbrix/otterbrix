//! Regression tests that mirror the `no_run` examples in the public docs.
//!
//! `cargo test --doc` only checks that `no_run` blocks compile; it never
//! actually runs them. The tests below copy the same code verbatim and
//! exercise it end-to-end against a live engine, so that drift between the
//! published examples and the real API is caught by `cargo test`.
//!
//! Only the storage path differs from the doc example: `./data` is replaced
//! with a unique temporary directory so the tests do not write into the
//! project tree and can run in parallel.
//!
//! **If you change one of these examples, update the matching test below.**

use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

use otterbrix::{Config, Database, SqlParam, SqlParamValue};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_dir(tag: &str) -> String {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!(
        "/tmp/otterbrix_examples_mirror_{tag}_{}_{id}",
        process::id()
    )
}

#[test]
fn lib_rs_quick_start() {
    let dir = unique_dir("quick_start");

    let cfg = Config::new(&dir);
    let db = Database::open(cfg).expect("open database");

    db.create_database("app").unwrap();
    db.create_collection("app", "t").unwrap();

    db.execute("INSERT INTO app.t (id, name) VALUES (1, 'alice');")
        .unwrap();

    let cursor = db.execute("SELECT id, name FROM app.t;").unwrap();
    let mut seen = Vec::new();
    for row in cursor.rows() {
        let id: i64 = row.get_by_name("id").get().unwrap();
        let name: String = row.get_by_name("name").get().unwrap();
        seen.push((id, name));
    }

    assert_eq!(seen, vec![(1_i64, "alice".to_string())]);
}

#[test]
fn database_rs_open_example() {
    let dir = unique_dir("open");

    let cfg = Config::new(&dir);
    let db = Database::open(cfg).expect("open database");
    db.create_database("app").unwrap();
}

#[test]
fn database_rs_execute_with_params_example() {
    let dir = unique_dir("execute_with_params");

    let db = Database::open(Config::new(&dir)).unwrap();
    db.create_database("app").unwrap();
    db.create_collection("app", "t").unwrap();

    let params = [
        SqlParam {
            index: 1,
            value: SqlParamValue::Int64(7),
        },
        SqlParam {
            index: 2,
            value: SqlParamValue::Str("ok"),
        },
    ];
    db.execute_with_params("INSERT INTO app.t (id, name) VALUES ($1, $2);", &params)
        .unwrap();
}
