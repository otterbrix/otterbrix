mod common;

use otterbrix::{Config, Database};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

static BUILDER_TEST_ID: AtomicUsize = AtomicUsize::new(0);

#[test]
fn open_and_drop() {
    let _db = common::open_test_db();
}

#[test]
fn open_multiple_instances() {
    let _a = common::open_test_db();
    let _b = common::open_test_db();
}

#[test]
fn open_with_config_builder() {
    let id = BUILDER_TEST_ID.fetch_add(1, Ordering::SeqCst);
    let base = format!("/tmp/otterbrix_safe_builder_{}_{id}", process::id());
    let config = Config::builder()
        .log_path(format!("{base}/log"))
        .wal_path(format!("{base}/wal"))
        .disk_path(format!("{base}/disk"))
        .main_path(format!("{base}/main"))
        .build();
    let _db = Database::open(config).expect("open with Config::builder");
}

#[test]
fn open_with_explicit_log_level() {
    let id = BUILDER_TEST_ID.fetch_add(1, Ordering::SeqCst);
    let base = format!("/tmp/otterbrix_safe_level_{}_{id}", process::id());
    let config = Config::builder()
        .level(2)
        .log_path(format!("{base}/log"))
        .wal_path(format!("{base}/wal"))
        .disk_path(format!("{base}/disk"))
        .main_path(format!("{base}/main"))
        .build();
    let db = Database::open(config).expect("open with explicit log level");
    db.execute("CREATE DATABASE leveldb;").unwrap();
    db.execute("CREATE TABLE leveldb.t (n integer);").unwrap();
}

#[test]
fn a_second_open_of_the_same_directory_is_refused() {
    let id = BUILDER_TEST_ID.fetch_add(1, Ordering::SeqCst);
    let dir = format!("/tmp/otterbrix_safe_locked_{}_{id}", process::id());
    let first = Database::open(Config::new(&dir)).expect("first open");
    match Database::open(Config::new(&dir)) {
        Err(otterbrix::Error::Open { code, message }) => {
            assert_ne!(code, 0);
            assert!(message.contains("unique directory"), "refusal: {message}");
        }
        other => panic!("a second open must be refused with the engine's reason, got {other:?}"),
    }
    drop(first);
    let reopened = Database::open(Config::new(&dir)).expect("open after the first engine is gone");
    reopened.execute("CREATE DATABASE relocked;").unwrap();
}
