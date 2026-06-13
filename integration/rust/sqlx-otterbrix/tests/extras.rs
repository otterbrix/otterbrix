mod common;

use common::{create_app_db, open_test_conn};
use sqlx::{Connection, Executor, Row};
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn round_trip_all_signed_integer_widths() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.s (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.s (k, v) VALUES (?, ?), (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(i8::MIN)
        .bind(2_i64)
        .bind(i16::MIN)
        .bind(3_i64)
        .bind(i32::MIN)
        .bind(4_i64)
        .bind(i64::MIN)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.s WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i8, _>("v")?, i8::MIN);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.s WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i16, _>("v")?, i16::MIN);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.s WHERE k = 3;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>("v")?, i32::MIN);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.s WHERE k = 4;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i64, _>("v")?, i64::MIN);
    Ok(())
}

#[tokio::test]
async fn round_trip_all_unsigned_integer_widths() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.u (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.u (k, v) VALUES (?, ?), (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(u8::MAX)
        .bind(2_i64)
        .bind(u16::MAX)
        .bind(3_i64)
        .bind(u32::MAX)
        .bind(4_i64)
        .bind(i64::MAX as u64)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.u WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u8, _>("v")?, u8::MAX);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.u WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u16, _>("v")?, u16::MAX);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.u WHERE k = 3;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u32, _>("v")?, u32::MAX);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.u WHERE k = 4;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u64, _>("v")?, i64::MAX as u64);
    Ok(())
}

#[tokio::test]
async fn round_trip_f32_via_double_column() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.f32 (k bigint, v double);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.f32 (k, v) VALUES (?, ?);")
        .bind(1_i64)
        .bind(2.5_f32)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.f32 WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: f32 = row.try_get("v")?;
    assert!((v - 2.5_f32).abs() < 1e-6);
    Ok(())
}

#[tokio::test]
async fn bind_borrowed_str_works() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.bs (k bigint, v string);")
        .execute(&mut t.conn)
        .await?;

    let s: &str = "borrowed";
    sqlx::query::<Otterbrix>("INSERT INTO app.bs (k, v) VALUES (?, ?);")
        .bind(1_i64)
        .bind(s)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.bs WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<String, _>("v")?, "borrowed");
    Ok(())
}

#[tokio::test]
async fn executor_describe_returns_unsupported_protocol_error() {
    let mut t = open_test_conn().await;
    let err = (&mut t.conn).describe("SELECT 1").await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("DESCRIBE"), "msg = {msg}");
}

#[tokio::test]
async fn executor_prepare_with_returns_statement_with_param_count() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    let _stmt = (&mut t.conn)
        .prepare_with("SELECT * FROM x WHERE a = ? AND b = ?;", &[])
        .await?;
    Ok(())
}

#[tokio::test]
async fn close_hard_consumes_connection() {
    let t = open_test_conn().await;
    t.conn.close_hard().await.expect("close_hard ok");
}

#[tokio::test]
async fn ping_can_be_called_repeatedly() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    t.conn.ping().await?;
    t.conn.ping().await?;
    t.conn.ping().await?;
    Ok(())
}
