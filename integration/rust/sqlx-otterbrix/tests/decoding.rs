mod common;

use common::{create_app_db, open_test_conn};
use sqlx::Row;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn decode_i64_from_double_truncates_toward_zero() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.f (k bigint, v double);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.f (k, v) VALUES (1, 3.7), (2, -2.9);")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.f WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: i64 = row.try_get("v")?;
    assert_eq!(v, 3);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.f WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: i64 = row.try_get("v")?;
    assert_eq!(v, -2);
    Ok(())
}

#[tokio::test]
async fn decode_i64_from_bool_via_try_get_unchecked() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.b (k bigint, v bool);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.b (k, v) VALUES (1, true), (2, false);")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.b WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: i64 = row.try_get_unchecked("v")?;
    assert_eq!(v, 1);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.b WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: i64 = row.try_get_unchecked("v")?;
    assert_eq!(v, 0);
    Ok(())
}

#[tokio::test]
async fn decode_bool_from_int_treats_zero_as_false() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.bi (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.bi (k, v) VALUES (1, 0), (2, 5), (3, -1);")
        .execute(&mut t.conn)
        .await?;

    for (k, expected) in [(1_i64, false), (2_i64, true), (3_i64, true)] {
        let row = sqlx::query::<Otterbrix>("SELECT v FROM app.bi WHERE k = ?;")
            .bind(k)
            .fetch_one(&mut t.conn)
            .await?;
        let v: bool = row.try_get("v")?;
        assert_eq!(v, expected, "row k={k}");
    }
    Ok(())
}

#[tokio::test]
async fn decode_f64_from_int_widens() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.fi (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.fi (k, v) VALUES (1, 7);")
        .execute(&mut t.conn)
        .await?;
    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.fi WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    let v: f64 = row.try_get("v")?;
    assert!((v - 7.0).abs() < 1e-9);
    Ok(())
}

#[tokio::test]
async fn decode_string_round_trips_utf8() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.ss (k bigint, v string);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.ss (k, v) VALUES (1, 'café'), (2, '日本語');")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.ss WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<String, _>("v")?, "café");

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.ss WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<String, _>("v")?, "日本語");

    Ok(())
}
