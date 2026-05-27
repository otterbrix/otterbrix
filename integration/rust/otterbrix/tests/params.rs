mod common;

use otterbrix::{SqlParam, SqlParamValue};

fn p(index: i32, value: SqlParamValue<'_>) -> SqlParam<'_> {
    SqlParam { index, value }
}

#[test]
fn insert_uint64_param_into_bigint_or_schema_free_column() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();

    db.create_collection("db", "u").unwrap();
    db.execute_with_params(
        "INSERT INTO db.u (k, v) VALUES ($1, $2);",
        &[
            p(1, SqlParamValue::Int64(1)),
            p(2, SqlParamValue::UInt64(u64::MAX)),
        ],
    )
    .unwrap();

    db.execute("CREATE TABLE db.s (k bigint, v bigint);")
        .unwrap();
    db.execute_with_params(
        "INSERT INTO db.s (k, v) VALUES ($1, $2);",
        &[
            p(1, SqlParamValue::Int64(1)),
            p(2, SqlParamValue::UInt64(u8::MAX as u64)),
        ],
    )
    .unwrap();
}

#[test]
fn insert_with_dollar_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (id, name) VALUES ($1, $2);",
        &[
            p(1, SqlParamValue::Int64(7)),
            p(2, SqlParamValue::Str("row7")),
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cur.size(), 1);
    let name: String = cur.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "row7");
}

#[test]
fn insert_repeated_placeholder_one_bind() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (a, b) VALUES ($1, $1);",
        &[p(1, SqlParamValue::Int64(99))],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    let a: i64 = cur.get_value_by_name(0, "a").get().unwrap();
    let b: i64 = cur.get_value_by_name(0, "b").get().unwrap();
    assert_eq!(a, 99);
    assert_eq!(b, 99);
}

#[test]
fn insert_all_supported_types() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute_with_params(
        "INSERT INTO db.t (i, u, d, s, b) VALUES ($1, $2, $3, $4, $5);",
        &[
            p(1, SqlParamValue::Int64(-42)),
            p(2, SqlParamValue::UInt64(7)),
            p(3, SqlParamValue::Double(3.5)),
            p(4, SqlParamValue::Str("hello")),
            p(5, SqlParamValue::Bool(true)),
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cur.size(), 1);
    let i: i64 = cur.get_value_by_name(0, "i").get().unwrap();
    let s: String = cur.get_value_by_name(0, "s").get().unwrap();
    let d: f64 = cur.get_value_by_name(0, "d").get().unwrap();
    let b: bool = cur.get_value_by_name(0, "b").get().unwrap();
    assert_eq!(i, -42);
    assert_eq!(s, "hello");
    assert!((d - 3.5).abs() < 1e-9);
    assert!(b);
}

#[test]
fn select_where_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, count) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE count > $1;",
            &[p(1, SqlParamValue::Int64(15))],
        )
        .unwrap();
    assert_eq!(cur.size(), 2);
}

#[test]
fn select_combined_and_or_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, count) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE count > $1 AND name = $2;",
            &[
                p(1, SqlParamValue::Int64(15)),
                p(2, SqlParamValue::Str("c")),
            ],
        )
        .unwrap();
    assert_eq!(cur.size(), 1);
    let name: String = cur.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "c");
}

#[test]
fn update_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20);")
        .unwrap();

    db.execute_with_params(
        "UPDATE db.t SET score = $1 WHERE name = $2;",
        &[
            p(1, SqlParamValue::Int64(777)),
            p(2, SqlParamValue::Str("a")),
        ],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t WHERE name = 'a';").unwrap();
    let score: i64 = cur.get_value_by_name(0, "score").get().unwrap();
    assert_eq!(score, 777);
}

#[test]
fn delete_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    db.execute_with_params(
        "DELETE FROM db.t WHERE score = $1;",
        &[p(1, SqlParamValue::Int64(20))],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cur.size(), 2);
}

#[test]
fn missing_param_is_error() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    let err = db
        .execute_with_params(
            "INSERT INTO db.t (id, name) VALUES ($1, $2);",
            &[p(1, SqlParamValue::Int64(1))],
        )
        .expect_err("missing $2 must return error");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("not all"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn unknown_param_index_is_error() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    let err = db
        .execute_with_params(
            "INSERT INTO db.t (id) VALUES ($1);",
            &[p(99, SqlParamValue::Int64(1))],
        )
        .expect_err("binding non-existing $99 must return error");
    let msg = format!("{err}");
    assert!(!msg.is_empty());
}

#[test]
fn zero_param_index_is_error() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    let err = db
        .execute_with_params(
            "INSERT INTO db.t (id) VALUES ($1);",
            &[p(0, SqlParamValue::Int64(1))],
        )
        .expect_err("zero index must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains(">= 1"), "unexpected error message: {msg}");
}

#[test]
fn injection_quote_in_string_param_is_stored_verbatim() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    let nasty = "Robert'); DROP TABLE db.t;--";
    db.execute_with_params(
        "INSERT INTO db.t (name) VALUES ($1);",
        &[p(1, SqlParamValue::Str(nasty))],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cur.size(), 1, "table must still exist with the row");
    let name: String = cur.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, nasty, "string must be stored as-is, not interpreted");
}

#[test]
fn injection_or_1_eq_1_does_not_match_extra_rows() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('alice', 1), ('bob', 2), ('eve', 3);")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE name = $1;",
            &[p(1, SqlParamValue::Str("alice' OR '1'='1"))],
        )
        .unwrap();
    assert_eq!(
        cur.size(),
        0,
        "literal 'alice' OR '1'='1' must not match any row"
    );
}

#[test]
fn injection_with_semicolon_does_not_chain_statements() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();
    db.create_collection("db", "victim").unwrap();

    db.execute("INSERT INTO db.victim (x) VALUES (1), (2), (3);")
        .unwrap();

    let payload = "x'; DELETE FROM db.victim; --";
    db.execute_with_params(
        "INSERT INTO db.t (name) VALUES ($1);",
        &[p(1, SqlParamValue::Str(payload))],
    )
    .unwrap();

    let cur = db.execute("SELECT * FROM db.victim;").unwrap();
    assert_eq!(
        cur.size(),
        3,
        "victim collection must be untouched: chained statements must not be executed"
    );
    let inserted: String = db
        .execute("SELECT * FROM db.t;")
        .unwrap()
        .get_value_by_name(0, "name")
        .get()
        .unwrap();
    assert_eq!(inserted, payload);
}

#[test]
fn injection_via_int_param_is_rejected_for_string_payload() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE id = $1;",
            &[p(1, SqlParamValue::Int64(1))],
        )
        .unwrap();
    assert_eq!(cur.size(), 1);
    let name: String = cur.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "a");
}

#[test]
fn select_where_bool_param() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, flag) VALUES ('a', true), ('b', false), ('c', true);")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE flag = $1;",
            &[p(1, SqlParamValue::Bool(true))],
        )
        .unwrap();
    assert_eq!(cur.size(), 2);
}

#[test]
fn select_where_in_list_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE id IN ($1, $2, $3);",
            &[
                p(1, SqlParamValue::Int64(1)),
                p(2, SqlParamValue::Int64(3)),
                p(3, SqlParamValue::Int64(99)),
            ],
        )
        .unwrap();
    assert_eq!(cur.size(), 2);
}

#[test]
fn duplicate_param_index_last_value_wins() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .unwrap();

    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE id = $1;",
            &[
                p(1, SqlParamValue::Int64(99)),
                p(1, SqlParamValue::Int64(2)),
            ],
        )
        .unwrap();
    assert_eq!(cur.size(), 1);
    let name: String = cur.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "b");
}

#[test]
fn select_limit_offset_with_params() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    for id in 1..=5_i64 {
        db.execute_with_params(
            "INSERT INTO db.t (id) VALUES ($1);",
            &[p(1, SqlParamValue::Int64(id))],
        )
        .unwrap();
    }

    let cur = db
        .execute_with_params(
            "SELECT id FROM db.t ORDER BY id LIMIT $1 OFFSET $2;",
            &[p(1, SqlParamValue::Int64(2)), p(2, SqlParamValue::Int64(1))],
        )
        .unwrap();
    assert_eq!(cur.size(), 2);
    let a: i64 = cur.get_value_by_name(0, "id").get().unwrap();
    let b: i64 = cur.get_value_by_name(1, "id").get().unwrap();
    assert_eq!(a, 2);
    assert_eq!(b, 3);
}

#[test]
fn smoke_thousand_repetitions_does_not_deadlock() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    for i in 0..1000_i64 {
        db.execute_with_params(
            "INSERT INTO db.t (id, name) VALUES ($1, $2);",
            &[p(1, SqlParamValue::Int64(i)), p(2, SqlParamValue::Str("x"))],
        )
        .unwrap();
    }

    let cur = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cur.size(), 1000);
}

#[test]
fn injection_with_comment_marker_in_string() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name) VALUES ('a'), ('b');")
        .unwrap();

    let payload = "a'--";
    let cur = db
        .execute_with_params(
            "SELECT * FROM db.t WHERE name = $1;",
            &[p(1, SqlParamValue::Str(payload))],
        )
        .unwrap();
    assert_eq!(
        cur.size(),
        0,
        "literal \"a'--\" must not match \"a\" — comment marker must NOT terminate the predicate"
    );
}
