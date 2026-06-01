//! Runtime mirror of the `no_run` doc-tests embedded in the public
//! documentation. Doc-tests with `no_run` only check that the example
//! compiles; this file actually executes the same code so that copy-paste
//! breakage is caught by the regular test suite.

use sqlx_core::connection::Connection;
use sqlx_core::row::Row;
use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};

#[tokio::test]
async fn lib_rs_quick_start() -> Result<(), sqlx_core::error::Error> {
    let dir = tempfile::tempdir().map_err(|e| sqlx_core::error::Error::Configuration(e.into()))?;

    let mut conn =
        OtterbrixConnection::connect_with(&OtterbrixConnectOptions::new(dir.path())).await?;

    sqlx_core::query::query::<Otterbrix>("CREATE DATABASE app;")
        .execute(&mut conn)
        .await?;
    sqlx_core::query::query::<Otterbrix>("CREATE TABLE app.t (id bigint, name string);")
        .execute(&mut conn)
        .await?;

    sqlx_core::query::query::<Otterbrix>("INSERT INTO app.t (id, name) VALUES (?, ?);")
        .bind(1_i64)
        .bind("alice")
        .execute(&mut conn)
        .await?;

    let row = sqlx_core::query::query::<Otterbrix>("SELECT name FROM app.t WHERE id = ?")
        .bind(1_i64)
        .fetch_one(&mut conn)
        .await?;

    let name: String = row.try_get("name")?;
    assert_eq!(name, "alice");

    Ok(())
}
