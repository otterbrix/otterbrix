#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::Connection;
use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fresh_dir() -> tempfile::TempDir {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    tempfile::Builder::new()
        .prefix(&format!("sqlx_otterbrix_test_{id}_"))
        .tempdir()
        .expect("tempdir")
}

pub struct TestConn {
    pub conn: OtterbrixConnection,
    pub _dir: tempfile::TempDir,
}

pub async fn open_test_conn() -> TestConn {
    let dir = fresh_dir();
    let conn = OtterbrixConnection::connect_with(&OtterbrixConnectOptions::new(dir.path()))
        .await
        .expect("open connection");
    TestConn { conn, _dir: dir }
}

pub async fn open_test_conn_tweak<F>(tweak: F) -> TestConn
where
    F: FnOnce(OtterbrixConnectOptions) -> OtterbrixConnectOptions,
{
    let dir = fresh_dir();
    let opts = tweak(OtterbrixConnectOptions::new(dir.path()));
    let conn = OtterbrixConnection::connect_with(&opts)
        .await
        .expect("open connection");
    TestConn { conn, _dir: dir }
}

pub async fn create_app_db(conn: &mut OtterbrixConnection) {
    use sqlx::Executor;

    conn.execute(sqlx::query::<Otterbrix>("CREATE DATABASE app;"))
        .await
        .expect("create database");
}
