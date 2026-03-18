mod common;

#[test]
fn create_database_returns_cursor() {
    let db = common::open_test_db();
    let cursor = db.create_database("mydb").unwrap();
    assert_eq!(cursor.size(), 0);
}

#[test]
fn create_collection_returns_cursor() {
    let db = common::open_test_db();
    db.create_database("mydb").unwrap();
    let cursor = db.create_collection("mydb", "users").unwrap();
    assert_eq!(cursor.size(), 0);
}

#[test]
fn create_table_via_sql() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE testdb;").unwrap();
    db.execute("CREATE TABLE testdb.items (name string, price bigint);")
        .unwrap();
}
