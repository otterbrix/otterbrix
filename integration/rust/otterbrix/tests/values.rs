mod common;

use otterbrix::{Error, SqlParam, SqlParamValue, Value};

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
fn extract_uint_value() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (u) VALUES ($1);",
        &[SqlParam {
            index: 1,
            value: SqlParamValue::UInt64(42),
        }],
    )
    .unwrap();

    let cursor = db.execute("SELECT u FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert!(
        val.is_uint(),
        "expected UInt from engine for unsigned column, got {:?}",
        val
    );
    assert_eq!(val.as_uint(), Some(42));
    let n: u64 = val.get().unwrap();
    assert_eq!(n, 42);
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

    db.execute("INSERT INTO db.t (val) VALUES (2.5);").unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let val = cursor.get_value(0, 0);
    assert!(val.is_double());
    let d = val.as_double().unwrap();
    assert!((d - 2.5).abs() < 1e-9);
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

#[test]
fn extract_i64_min_and_max() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (lo, hi) VALUES ($1, $2);",
        &[
            SqlParam {
                index: 1,
                value: SqlParamValue::Int64(i64::MIN),
            },
            SqlParam {
                index: 2,
                value: SqlParamValue::Int64(i64::MAX),
            },
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT lo, hi FROM db.t;").unwrap();
    let lo: i64 = cur.get_value_by_name(0, "lo").get().unwrap();
    let hi: i64 = cur.get_value_by_name(0, "hi").get().unwrap();
    assert_eq!(lo, i64::MIN);
    assert_eq!(hi, i64::MAX);
}

#[test]
fn extract_u64_max() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (u) VALUES ($1);",
        &[SqlParam {
            index: 1,
            value: SqlParamValue::UInt64(u64::MAX),
        }],
    )
    .unwrap();

    let cur = db.execute("SELECT u FROM db.t;").unwrap();
    let val = cur.get_value(0, 0);
    assert!(
        val.is_uint(),
        "expected UInt for u64::MAX round-trip, got {val:?}"
    );
    assert_eq!(val.as_uint(), Some(u64::MAX));
}

#[test]
fn extract_empty_string() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (s) VALUES ($1);",
        &[SqlParam {
            index: 1,
            value: SqlParamValue::Str(""),
        }],
    )
    .unwrap();

    let cur = db.execute("SELECT s FROM db.t;").unwrap();
    let s: String = cur.get_value_by_name(0, "s").get().unwrap();
    assert_eq!(s, "");
}

#[test]
fn extract_double_infinity() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (a, b) VALUES ($1, $2);",
        &[
            SqlParam {
                index: 1,
                value: SqlParamValue::Double(f64::INFINITY),
            },
            SqlParam {
                index: 2,
                value: SqlParamValue::Double(f64::NEG_INFINITY),
            },
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT a, b FROM db.t;").unwrap();
    let a: f64 = cur.get_value_by_name(0, "a").get().unwrap();
    let b: f64 = cur.get_value_by_name(0, "b").get().unwrap();
    assert!(a.is_infinite() && a.is_sign_positive(), "got {a}");
    assert!(b.is_infinite() && b.is_sign_negative(), "got {b}");
}

#[test]
fn extract_double_nan() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (x) VALUES ($1);",
        &[SqlParam {
            index: 1,
            value: SqlParamValue::Double(f64::NAN),
        }],
    )
    .unwrap();

    let cur = db.execute("SELECT x FROM db.t;").unwrap();
    let x: f64 = cur.get_value_by_name(0, "x").get().unwrap();
    assert!(x.is_nan(), "expected NaN round-trip, got {x}");
}

#[test]
fn extract_utf8_strings() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (ru, jp, emoji) VALUES ($1, $2, $3);",
        &[
            SqlParam {
                index: 1,
                value: SqlParamValue::Str("Москва"),
            },
            SqlParam {
                index: 2,
                value: SqlParamValue::Str("日本"),
            },
            SqlParam {
                index: 3,
                value: SqlParamValue::Str("🦀 Rust"),
            },
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT ru, jp, emoji FROM db.t;").unwrap();
    let ru: String = cur.get_value_by_name(0, "ru").get().unwrap();
    let jp: String = cur.get_value_by_name(0, "jp").get().unwrap();
    let emoji: String = cur.get_value_by_name(0, "emoji").get().unwrap();
    assert_eq!(ru, "Москва");
    assert_eq!(jp, "日本");
    assert_eq!(emoji, "🦀 Rust");
}
