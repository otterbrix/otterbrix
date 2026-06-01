//! Regression tests that mirror the runnable examples in the public docs.
//!
//! The quick-start in `src/lib.rs` and the `OtterbrixProxy` example in
//! `src/proxy.rs` are marked `no_run`, so `cargo test --doc` only verifies
//! that they compile. These tests run the same code end-to-end against a
//! live Otterbrix instance to make sure the published examples actually
//! work. **If you change either example, update the matching test below.**

use std::sync::Arc;

use otterbrix::{Config, Database};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, ProxyDatabaseTrait, Statement};
use seaorm_otterbrix::OtterbrixProxy;
use tempfile::tempdir;

#[tokio::test]
async fn lib_rs_quick_start() {
    let tmp = tempdir().expect("tempdir");

    let db = Database::open(Config::new(tmp.path())).expect("open database");
    db.create_database("app").expect("create database");
    db.create_collection("app", "t").expect("create collection");

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    let conn: DatabaseConnection = sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy)
        .await
        .expect("connect_proxy");

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (1, 'alice');".to_string(),
    ))
    .await
    .expect("execute insert");
}

#[tokio::test]
async fn proxy_rs_otterbrix_proxy_example() {
    let tmp = tempdir().expect("tempdir");

    let db = Database::open(Config::new(tmp.path())).expect("open database");
    db.create_database("app").expect("create database");
    db.create_collection("app", "t").expect("create collection");

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    let _conn: DatabaseConnection = sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy)
        .await
        .expect("connect_proxy");
}
