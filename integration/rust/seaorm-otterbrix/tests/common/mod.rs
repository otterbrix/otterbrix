#![allow(dead_code)]

use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use otterbrix::{Config, Database};
use sea_orm::{DatabaseConnection, DbBackend, ProxyDatabaseTrait};
use seaorm_otterbrix::OtterbrixProxy;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn open_test_db() -> Database {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = format!("/tmp/seaorm_otterbrix_test_{}_{id}", process::id());
    let config = Config::new(&dir);
    Database::open(config).expect("failed to open database")
}

pub async fn open_test_proxy() -> DatabaseConnection {
    let db = open_test_db();
    db.create_database("app").expect("create database");
    db.create_collection("app", "t").expect("create collection");

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy)
        .await
        .expect("connect proxy")
}
