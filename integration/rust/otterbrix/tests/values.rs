mod common;

use otterbrix::{Error, Value};

#[test]
fn extract_string_value() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name) VALUES ('hello');")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert_eq!(val, Value::String("hello".into()));
    assert!(val.is_string());
    assert_eq!(val.as_str(), Some("hello"));
}

#[test]
fn extract_integer_value() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (num) VALUES (42);").unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert!(val.is_int());
    assert_eq!(val.as_int(), Some(42));
}

#[test]
fn extract_bool_value() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();
    db.execute("CREATE TABLE db.t (flag boolean);").unwrap();

    db.execute("INSERT INTO db.t (flag) VALUES (true);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert!(val.is_bool());
    assert_eq!(val.as_bool(), Some(true));
}

#[test]
fn extract_double_value() {
    let db = common::open_test_db();
    db.execute("CREATE DATABASE db;").unwrap();
    db.execute("CREATE TABLE db.t (val double);").unwrap();

    db.execute("INSERT INTO db.t (val) VALUES (3.14);").unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert!(val.is_double());
    let d = val.as_double().unwrap();
    assert!((d - 3.14).abs() < 1e-9);
}

#[test]
fn column_names() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Alice', 30);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.column_count(), 2);
    assert_eq!(cursor.column_name(0).as_deref(), Some("name"));
    assert_eq!(cursor.column_name(1).as_deref(), Some("age"));
    assert!(cursor.column_name(99).is_none());
}

#[test]
fn get_value_by_name() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (city, pop) VALUES ('Moscow', 12000000);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let city = cursor.get_value_by_name(0, "city");
    assert_eq!(city, Value::String("Moscow".into()));

    let pop = cursor.get_value_by_name(0, "pop");
    assert!(pop.is_int());

    let missing = cursor.get_value_by_name(0, "nonexistent");
    assert!(missing.is_null());
}

#[test]
fn from_value_typed_extraction() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, count) VALUES ('test', 99);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let name: String = cursor.get_value_by_name(0, "name").get().unwrap();
    let count: i64 = cursor.get_value_by_name(0, "count").get().unwrap();
    assert_eq!(name, "test");
    assert_eq!(count, 99);
}

#[test]
fn from_value_type_mismatch() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name) VALUES ('hello');")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    let result = val.get::<i64>();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::TypeMismatch { expected, got } => {
            assert_eq!(expected, "Int");
            assert_eq!(got, "String");
        }
        other => panic!("expected TypeMismatch, got: {other}"),
    }
}

#[test]
fn value_type_checkers_and_accessors() {
    let v = Value::Int(7);
    assert!(v.is_int());
    assert!(!v.is_bool());
    assert!(!v.is_null());
    assert!(!v.is_string());
    assert!(!v.is_double());
    assert!(!v.is_uint());
    assert_eq!(v.as_int(), Some(7));
    assert_eq!(v.as_bool(), None);
    assert_eq!(v.as_str(), None);
}

#[test]
fn value_display() {
    assert_eq!(format!("{}", Value::Null), "NULL");
    assert_eq!(format!("{}", Value::Bool(true)), "true");
    assert_eq!(format!("{}", Value::Int(42)), "42");
    assert_eq!(format!("{}", Value::String("hi".into())), "hi");
}
