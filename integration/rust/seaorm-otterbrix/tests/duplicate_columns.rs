use std::sync::Arc;

use otterbrix::{Config, Database};
use sea_orm::{ConnectionTrait, DbBackend, ProxyDatabaseTrait, Statement};
use seaorm_otterbrix::{positional_proxy_column_key, OtterbrixProxy};

async fn open_proxy_with_two_tables() -> sea_orm::DatabaseConnection {
    let dir = format!(
        "/tmp/seaorm_otterbrix_dup_cols_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let db = Database::open(Config::new(&dir)).expect("open");
    db.create_database("app").expect("create db");
    db.execute("CREATE TABLE app.t1 (id bigint, val bigint);")
        .expect("ddl t1");
    db.execute("CREATE TABLE app.t2 (id bigint, label string);")
        .expect("ddl t2");
    db.execute("INSERT INTO app.t1 (id, val) VALUES (1, 10), (2, 20);")
        .expect("seed t1");
    db.execute("INSERT INTO app.t2 (id, label) VALUES (1, 'a'), (2, 'b');")
        .expect("seed t2");

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy)
        .await
        .expect("connect proxy")
}

#[tokio::test]
async fn join_with_duplicate_id_column_uses_positional_keys() {
    let conn = open_proxy_with_two_tables().await;

    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT t1.id, t2.id FROM app.t1 INNER JOIN app.t2 ON t1.id = t2.id ORDER BY t1.id ASC;"
            .to_string(),
    );
    let rows = conn.query_all(stmt).await.expect("inner join select");

    assert_eq!(rows.len(), 2, "expected 2 joined rows, got {}", rows.len());

    let key0 = positional_proxy_column_key(0);
    let key1 = positional_proxy_column_key(1);

    let r0_left: i64 = rows[0]
        .try_get("", &key0)
        .expect("left id at positional key 0");
    let r0_right: i64 = rows[0]
        .try_get("", &key1)
        .expect("right id at positional key 1");
    assert_eq!(r0_left, r0_right, "JOIN ON t1.id = t2.id keeps ids equal");

    let r1_left: i64 = rows[1]
        .try_get("", &key0)
        .expect("row 1 left id at positional key 0");
    let r1_right: i64 = rows[1]
        .try_get("", &key1)
        .expect("row 1 right id at positional key 1");
    assert_eq!(r1_left, r1_right);
    assert_ne!(r0_left, r1_left, "two distinct rows expected");
}
