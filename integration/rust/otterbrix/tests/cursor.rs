mod common;

#[test]
fn has_next_is_true_when_select_returns_rows() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();
    db.execute("INSERT INTO db.t (x) VALUES (1), (2), (3);")
        .unwrap();

    let cursor = db.execute("SELECT x FROM db.t;").unwrap();
    assert!(
        cursor.has_next(),
        "fresh cursor over non-empty result must report has_next == true"
    );
    assert_eq!(cursor.size(), 3);
}

#[test]
fn a_write_reports_affected_rows_and_no_result_rows() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE wdb;").unwrap();
    db.execute("CREATE TABLE wdb.t (x bigint);").unwrap();

    let inserted = db
        .execute("INSERT INTO wdb.t (x) VALUES (1), (2), (3);")
        .unwrap();
    assert_eq!(inserted.affected_rows(), Some(3));
    assert_eq!(inserted.size(), 0);

    let deleted = db.execute("DELETE FROM wdb.t WHERE x > 5;").unwrap();
    assert_eq!(deleted.affected_rows(), Some(0));

    let selected = db.execute("SELECT x FROM wdb.t;").unwrap();
    assert_eq!(selected.affected_rows(), None);
    assert_eq!(selected.size(), 3);
}

#[test]
fn column_logical_type_returns_none_for_negative_index() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE typedb;").unwrap();
    db.execute("CREATE TABLE typedb.t (n integer);").unwrap();
    db.execute("INSERT INTO typedb.t (n) VALUES (1);").unwrap();
    let cur = db.execute("SELECT n FROM typedb.t;").unwrap();
    assert_eq!(cur.column_logical_type(-1), None);
}

#[test]
fn column_logical_type_returns_none_for_out_of_bounds_index() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE typedb;").unwrap();
    db.execute("CREATE TABLE typedb.t (n integer);").unwrap();
    db.execute("INSERT INTO typedb.t (n) VALUES (1);").unwrap();
    let cur = db.execute("SELECT n FROM typedb.t;").unwrap();
    assert_eq!(cur.column_count(), 1);
    assert_eq!(cur.column_logical_type(100), None);
}

#[test]
fn column_name_returns_none_for_out_of_bounds_index() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE namedb;").unwrap();
    db.execute("CREATE TABLE namedb.t (n integer);").unwrap();
    db.execute("INSERT INTO namedb.t (n) VALUES (1);").unwrap();
    let cur = db.execute("SELECT n FROM namedb.t;").unwrap();
    assert_eq!(cur.column_count(), 1);
    assert_eq!(cur.column_name(100), None);
}
