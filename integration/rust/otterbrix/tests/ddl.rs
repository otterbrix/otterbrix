use otterbrix::LOGICAL_TYPE_INTEGER;

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

#[test]
fn drop_collection_then_database() {
    let db = common::open_test_db();
    db.create_database("dropme").unwrap();
    db.create_collection("dropme", "t").unwrap();
    db.drop_collection("dropme", "t").unwrap();
    db.drop_database("dropme").unwrap();
}

#[test]
fn cursor_reports_integer_logical_type() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE typedb;").unwrap();
    db.execute("CREATE TABLE typedb.nums (n integer);").unwrap();
    db.execute("INSERT INTO typedb.nums (n) VALUES (42);").unwrap();
    let cur = db.execute("SELECT n FROM typedb.nums;").unwrap();
    assert_eq!(cur.column_count(), 1);
    assert_eq!(cur.column_logical_type(0), Some(LOGICAL_TYPE_INTEGER));
}
