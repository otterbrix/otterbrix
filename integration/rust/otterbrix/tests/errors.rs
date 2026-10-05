mod common;

use otterbrix::Error;

#[test]
fn select_from_nonexistent_table() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();
    let result = db.execute("SELECT * FROM db.nonexistent;");
    assert!(result.is_err());
}

#[test]
fn invalid_sql_returns_error() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();
    assert!(db.execute("THIS IS NOT SQL;").is_err());
    assert!(db.execute("SELECT * FROM;").is_err());
}

#[test]
fn query_error_carries_code_and_message() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();

    let err = db
        .execute("SELECT * FROM db.nonexistent;")
        .expect_err("query against missing table must error");
    match err {
        Error::Query { code, message } => {
            assert_ne!(code, 0, "expected non-zero error code, got {code}");
            assert!(!message.is_empty(), "expected non-empty error message");
        }
        other => panic!("expected Error::Query, got {other}"),
    }
}

#[test]
fn create_database_twice_returns_error() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE dup;").unwrap();
    let res = db.execute("CREATE DATABASE dup;");
    assert!(
        res.is_err(),
        "creating an already-existing database must return Err"
    );
}

#[test]
fn drop_nonexistent_collection_returns_error() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE d;").unwrap();
    let res = db.execute("DROP TABLE d.missing;");
    assert!(
        res.is_err(),
        "dropping a missing collection must return Err"
    );
}

#[test]
fn drop_nonexistent_database_returns_error() {
    let db = common::open_test_db();
    let res = db.execute("DROP DATABASE never_created;");
    assert!(res.is_err(), "dropping a missing database must return Err");
}
