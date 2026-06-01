mod common;

use common::open_test_db;
use otterbrix::{Database, SqlParam, SqlParamValue};

fn count_rows(db: &Database, table: &str) -> i64 {
    let cursor = db
        .execute(&format!("SELECT COUNT(k) AS cnt FROM {table};"))
        .expect("count");
    assert_eq!(cursor.size(), 1);
    let v = cursor.get_value(0, 0);
    v.as_int()
        .or_else(|| v.as_uint().map(|x| x as i64))
        .expect("count is int or uint")
}

#[test]
fn insert_many_rows_in_batches() {
    let db = open_test_db();
    db.create_database("app").expect("create database");
    db.execute("CREATE TABLE app.t (k bigint, v bigint);")
        .expect("create table");

    const TOTAL: i64 = 20_000;
    const BATCH: i64 = 500;

    let mut k = 0i64;
    while k < TOTAL {
        let end = (k + BATCH).min(TOTAL);
        let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
        let mut first = true;
        for i in k..end {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("({i}, {})", i * 3));
        }
        sql.push(';');
        db.execute(&sql).expect("batch insert");
        k = end;
    }

    assert_eq!(count_rows(&db, "app.t"), TOTAL);

    let cursor = db
        .execute("SELECT v FROM app.t WHERE k = 12345;")
        .expect("point select");
    assert_eq!(cursor.size(), 1);
    assert_eq!(cursor.get_value(0, 0).as_int(), Some(12345 * 3));
}

#[test]
fn large_select_returns_all_rows() {
    let db = open_test_db();
    db.create_database("app").expect("create database");
    db.execute("CREATE TABLE app.t (k bigint);")
        .expect("create table");

    const TOTAL: i64 = 5_000;
    let mut sql = String::from("INSERT INTO app.t (k) VALUES ");
    for i in 0..TOTAL {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    sql.push(';');
    db.execute(&sql).expect("bulk insert");

    let cursor = db.execute("SELECT k FROM app.t;").expect("select all");
    assert_eq!(cursor.size() as i64, TOTAL);

    let mut sum: i64 = 0;
    for row in cursor.rows() {
        sum += row.get(0).as_int().expect("int");
    }
    assert_eq!(sum, (0..TOTAL).sum::<i64>());
}

#[test]
fn many_open_close_iterations_do_not_leak_resources() {
    for _ in 0..50 {
        let db = open_test_db();
        db.create_database("app").expect("create database");
        db.execute("CREATE TABLE app.t (k bigint);")
            .expect("create table");
        for k in 0..20_i64 {
            db.execute(&format!("INSERT INTO app.t (k) VALUES ({k});"))
                .expect("insert");
        }
        assert_eq!(count_rows(&db, "app.t"), 20);
    }
}

#[test]
fn many_moderate_strings_round_trip() {
    let db = open_test_db();
    db.create_database("app").expect("create database");
    db.execute("CREATE TABLE app.t (k bigint, v string);")
        .expect("create table");

    let payload: String = "a".repeat(2 * 1024);
    const ROWS: i64 = 200;
    for k in 0..ROWS {
        let params = [
            SqlParam {
                index: 1,
                value: SqlParamValue::Int64(k),
            },
            SqlParam {
                index: 2,
                value: SqlParamValue::Str(&payload),
            },
        ];
        db.execute_with_params("INSERT INTO app.t (k, v) VALUES ($1, $2);", &params)
            .expect("insert");
    }

    let cursor = db.execute("SELECT v FROM app.t;").expect("select all");
    assert_eq!(cursor.size() as i64, ROWS);
    for row in cursor.rows() {
        let s = row.get(0).as_str().map(String::from).expect("string");
        assert_eq!(s.len(), payload.len());
    }
}

#[test]
fn many_repeated_point_queries() {
    let db = open_test_db();
    db.create_database("app").expect("create database");
    db.execute("CREATE TABLE app.t (k bigint, v bigint);")
        .expect("create table");

    let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
    for k in 0..200 {
        if k > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({k}, {})", k * 2));
    }
    sql.push(';');
    db.execute(&sql).expect("seed");

    for _ in 0..1_000 {
        let cursor = db
            .execute("SELECT v FROM app.t WHERE k = 137;")
            .expect("point select");
        assert_eq!(cursor.size(), 1);
        assert_eq!(cursor.get_value(0, 0).as_int(), Some(274));
    }
}
