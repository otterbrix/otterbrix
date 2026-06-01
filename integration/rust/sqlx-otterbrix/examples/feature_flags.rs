//! Feature-flag store accessed through SQLx.
//!
//! Demonstrates the SQLx workflow on top of the Otterbrix driver:
//!
//! 1. open a connection through [`OtterbrixConnection::connect_with`];
//! 2. issue DDL via `sqlx::query::<Otterbrix>("...").execute(&mut conn)`;
//! 3. insert rows with positional `?` placeholders bound through
//!    [`Query::bind`];
//! 4. fetch a single row with [`Query::fetch_one`];
//! 5. fetch many rows and read columns with [`Row::try_get`].
//!
//! The data model is a tiny feature-flag table: each flag has a key, an
//! enabled bit and a percentage rollout 0..=100.
//!
//! Run with `cargo run --example feature_flags`.
//!
//! [`Query::bind`]: sqlx::query::Query::bind
//! [`Query::fetch_one`]: sqlx::query::Query::fetch_one
//! [`Row::try_get`]: sqlx::Row::try_get
//! [`OtterbrixConnection::connect_with`]: sqlx_otterbrix::OtterbrixConnection::connect_with

use std::path::Path;

use sqlx::{ConnectOptions, Connection, Row};
use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};

#[allow(dead_code)] // included by tests/examples_run.rs via #[path]; main is then unused.
fn main() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(err) = rt.block_on(run(tmp.path())) {
        eprintln!("example failed: {err}");
        std::process::exit(1);
    }
}

pub async fn run(data_dir: &Path) -> Result<(), sqlx::Error> {
    let mut conn = OtterbrixConnectOptions::new(data_dir).connect().await?;

    sqlx::query::<Otterbrix>("CREATE DATABASE flags;")
        .execute(&mut conn)
        .await?;
    sqlx::query::<Otterbrix>(
        "CREATE TABLE flags.entries (key string, enabled bool, rollout bigint);",
    )
    .execute(&mut conn)
    .await?;

    insert_flag(&mut conn, "new_dashboard", true, 100).await?;
    insert_flag(&mut conn, "dark_mode", true, 50).await?;
    insert_flag(&mut conn, "experimental_search", false, 0).await?;
    insert_flag(&mut conn, "beta_invoicing", true, 10).await?;

    println!("=== current feature flags ===");
    let rows = sqlx::query::<Otterbrix>("SELECT key, enabled, rollout FROM flags.entries;")
        .fetch_all(&mut conn)
        .await?;
    for row in &rows {
        let key: String = row.try_get("key")?;
        let enabled: bool = row.try_get("enabled")?;
        let rollout: i64 = row.try_get("rollout")?;
        let mark = if enabled { "on " } else { "off" };
        println!("  [{mark}] {key:<22} rollout={rollout}%");
    }

    let key = "dark_mode";
    let row = sqlx::query::<Otterbrix>("SELECT enabled, rollout FROM flags.entries WHERE key = ?;")
        .bind(key)
        .fetch_one(&mut conn)
        .await?;
    let enabled: bool = row.try_get("enabled")?;
    let rollout: i64 = row.try_get("rollout")?;
    println!("\nlookup '{key}' -> enabled={enabled}, rollout={rollout}%");

    conn.close().await?;
    Ok(())
}

async fn insert_flag(
    conn: &mut OtterbrixConnection,
    key: &str,
    enabled: bool,
    rollout: i64,
) -> Result<(), sqlx::Error> {
    // `Query::bind` requires its arguments to outlive the executor; pass an
    // owned `String` rather than the borrowed `&str` to satisfy that bound.
    sqlx::query::<Otterbrix>("INSERT INTO flags.entries (key, enabled, rollout) VALUES (?, ?, ?);")
        .bind(key.to_owned())
        .bind(enabled)
        .bind(rollout)
        .execute(conn)
        .await?;
    Ok(())
}
