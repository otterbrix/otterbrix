mod common;

use sea_orm::{sea_query::Value as SeaValue, ConnectionTrait, DbBackend, Statement};

#[tokio::test]
async fn roundtrip_basic_types() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (i, u, d, s, b) VALUES ($1, $2, $3, $4, $5);",
        vec![
            SeaValue::BigInt(Some(-12345)),
            SeaValue::BigUnsigned(Some(7)),
            SeaValue::Double(Some(2.75)),
            SeaValue::String(Some(Box::new("hello world".to_string()))),
            SeaValue::Bool(Some(true)),
        ],
    );
    conn.execute(stmt).await.expect("insert");

    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT i, u, d, s, b FROM app.t;".to_string(),
    );
    let rows = conn.query_all(stmt).await.expect("select");
    assert_eq!(rows.len(), 1);

    let i: i64 = rows[0].try_get("", "i").unwrap();
    let u: u64 = rows[0].try_get("", "u").unwrap();
    let d: f64 = rows[0].try_get("", "d").unwrap();
    let s: String = rows[0].try_get("", "s").unwrap();
    let b: bool = rows[0].try_get("", "b").unwrap();

    assert_eq!(i, -12345);
    assert_eq!(u, 7);
    assert!((d - 2.75).abs() < 1e-9);
    assert_eq!(s, "hello world");
    assert!(b);
}

#[tokio::test]
async fn small_int_widens_to_bigint() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (id) VALUES ($1);",
        vec![SeaValue::Int(Some(1234))],
    );
    conn.execute(stmt).await.expect("insert");

    let stmt = Statement::from_string(DbBackend::Postgres, "SELECT id FROM app.t;".to_string());
    let rows = conn.query_all(stmt).await.expect("select");
    let id: i64 = rows[0].try_get("", "id").unwrap();
    assert_eq!(id, 1234);
}

#[tokio::test]
async fn float_widens_to_double() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (rate) VALUES ($1);",
        vec![SeaValue::Float(Some(1.5))],
    );
    conn.execute(stmt).await.expect("insert");

    let stmt = Statement::from_string(DbBackend::Postgres, "SELECT rate FROM app.t;".to_string());
    let rows = conn.query_all(stmt).await.expect("select");
    let rate: f64 = rows[0].try_get("", "rate").unwrap();
    assert!((rate - 1.5).abs() < 1e-6);
}
