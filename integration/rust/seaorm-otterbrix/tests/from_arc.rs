mod common;

use std::sync::Arc;

use otterbrix::{Config, Database};
use parking_lot::Mutex;
use sea_orm::{ConnectionTrait, DbBackend, ProxyDatabaseTrait, Statement};
use seaorm_otterbrix::OtterbrixProxy;

#[tokio::test]
async fn from_arc_shares_database_with_outside_code() {
    let dir = format!(
        "/tmp/seaorm_otterbrix_from_arc_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let db = Database::open(Config::new(&dir)).expect("open");
    let shared: Arc<Mutex<Database>> = Arc::new(Mutex::new(db));

    {
        let guard = shared.lock();
        guard.create_database("app").expect("create database");
        guard
            .create_collection("app", "t")
            .expect("create collection");
        guard
            .execute("CREATE TABLE app.shared_tbl (id bigint, name string);")
            .expect("ddl via direct arc");
    }

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> =
        Arc::new(Box::new(OtterbrixProxy::from_arc(Arc::clone(&shared))));
    let conn = sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy)
        .await
        .expect("connect proxy");

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.shared_tbl (id, name) VALUES (1, 'shared');".to_string(),
    ))
    .await
    .expect("insert via proxy");

    let guard = shared.lock();
    let cursor = guard
        .execute("SELECT id, name FROM app.shared_tbl;")
        .expect("select via direct arc");
    assert_eq!(cursor.size(), 1);
}
