mod common;

use common::{create_app_db, open_test_conn};
use sqlx::Row;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn round_trip_signed_integers() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.ints (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.ints (k, v) VALUES (?, ?), (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(i64::MIN)
        .bind(2_i64)
        .bind(i64::MAX)
        .bind(3_i64)
        .bind(0_i64)
        .bind(4_i64)
        .bind(-1_i64)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.ints WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i64, _>("v")?, i64::MIN);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.ints WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i64, _>("v")?, i64::MAX);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.ints WHERE k = ?")
        .bind(4_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i64, _>("v")?, -1_i64);

    Ok(())
}

#[tokio::test]
async fn round_trip_narrow_signed_integers() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.narrow (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.narrow (k, v) VALUES (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(127_i8)
        .bind(2_i64)
        .bind(32767_i16)
        .bind(3_i64)
        .bind(-2147483648_i32)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.narrow WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i8, _>("v")?, 127_i8);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.narrow WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i16, _>("v")?, 32767_i16);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.narrow WHERE k = ?")
        .bind(3_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>("v")?, -2147483648_i32);

    Ok(())
}

#[tokio::test]
async fn round_trip_unsigned_integers() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.uints (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.uints (k, v) VALUES (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(0_u64)
        .bind(2_i64)
        .bind(i64::MAX as u64)
        .bind(3_i64)
        .bind(42_u32)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.uints WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u64, _>("v")?, i64::MAX as u64);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.uints WHERE k = ?")
        .bind(3_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<u32, _>("v")?, 42_u32);

    Ok(())
}

#[tokio::test]
async fn round_trip_floats() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.floats (k bigint, v double);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.floats (k, v) VALUES (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(3.5_f64)
        .bind(2_i64)
        .bind(-0.0_f64)
        .bind(3_i64)
        .bind(1.5_f32)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.floats WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    let v: f64 = row.try_get("v")?;
    assert!((v - 3.5).abs() < 1e-9);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.floats WHERE k = ?")
        .bind(3_i64)
        .fetch_one(&mut t.conn)
        .await?;
    let v: f32 = row.try_get("v")?;
    assert!((v - 1.5_f32).abs() < 1e-6);

    Ok(())
}

#[tokio::test]
async fn round_trip_bool() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.bools (k bigint, v bool);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.bools (k, v) VALUES (?, ?), (?, ?);")
        .bind(1_i64)
        .bind(true)
        .bind(2_i64)
        .bind(false)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.bools WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert!(row.try_get::<bool, _>("v")?);

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.bools WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert!(!row.try_get::<bool, _>("v")?);

    Ok(())
}

#[tokio::test]
async fn round_trip_string_including_utf8() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.strs (k bigint, v string);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.strs (k, v) VALUES (?, ?), (?, ?), (?, ?);")
        .bind(1_i64)
        .bind("hello")
        .bind(2_i64)
        .bind("Привет, мир!".to_owned())
        .bind(3_i64)
        .bind("")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.strs WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<String, _>("v")?, "Привет, мир!");

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.strs WHERE k = ?")
        .bind(3_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<String, _>("v")?, "");

    Ok(())
}

#[tokio::test]
async fn option_decodes_null_as_none_and_value_as_some() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.opt (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO app.opt (k, v) VALUES (1, 7), (2, NULL);")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.opt WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    let some: Option<i64> = row.try_get("v")?;
    assert_eq!(some, Some(7));

    let row = sqlx::query::<Otterbrix>("SELECT * FROM app.opt WHERE k = ?")
        .bind(2_i64)
        .fetch_one(&mut t.conn)
        .await?;
    let none: Option<i64> = row.try_get("v")?;
    assert_eq!(none, None);

    Ok(())
}

#[tokio::test]
async fn encode_option_some_round_trips() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.eopt (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;

    let some: Option<i64> = Some(11);
    sqlx::query::<Otterbrix>("INSERT INTO app.eopt (k, v) VALUES (?, ?);")
        .bind(1_i64)
        .bind(some)
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.eopt WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    let v: Option<i64> = row.try_get("v")?;
    assert_eq!(v, Some(11));

    Ok(())
}
