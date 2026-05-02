mod common;

#[test]
fn select_from_nonexistent_table() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    let result = db.execute("SELECT * FROM db.nonexistent;");
    assert!(result.is_err());
}

#[test]
fn invalid_sql_returns_error() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    assert!(db.execute("THIS IS NOT SQL;").is_err());
    assert!(db.execute("SELECT * FROM;").is_err());
}
