mod common;

#[test]
fn has_next_is_true_when_select_returns_rows() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();
    db.execute("CREATE TABLE db.t();").unwrap();
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
fn create_table_reports_its_oid() {
    let db = common::open_test_db();
    let database = db.execute("CREATE DATABASE odb;").unwrap();
    assert_eq!(database.created_oid(), None);

    let created = db
        .execute("CREATE TABLE odb.t (x bigint);")
        .unwrap()
        .created_oid()
        .expect("CREATE TABLE answers the oid it created");

    let catalog = db
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 't';")
        .unwrap();
    assert_eq!(catalog.created_oid(), None);
    assert_eq!(catalog.size(), 1);
    assert_eq!(catalog.get_value(0, 0).as_uint(), Some(u64::from(created)));
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
