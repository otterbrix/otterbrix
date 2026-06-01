use sqlx::Connection;
use sqlx::Row;
use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};

#[tokio::test]
async fn insert_select_and_param() -> Result<(), sqlx::Error> {
    let dir = tempfile::tempdir().map_err(|e| sqlx::Error::Configuration(e.into()))?;
    let mut conn =
        OtterbrixConnection::connect_with(&OtterbrixConnectOptions::new(dir.path())).await?;

    sqlx::query::<Otterbrix>("CREATE DATABASE db;")
        .execute(&mut conn)
        .await?;
    sqlx::query::<Otterbrix>("CREATE TABLE db.items (name string, qty bigint);")
        .execute(&mut conn)
        .await?;

    sqlx::query::<Otterbrix>("INSERT INTO db.items (name, qty) VALUES ('a', 10), ('b', 20);")
        .execute(&mut conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT name FROM db.items WHERE qty = ?")
        .bind(20_i64)
        .fetch_one(&mut conn)
        .await?;

    let name: String = row.try_get("name")?;
    assert_eq!(name, "b");

    Ok(())
}
