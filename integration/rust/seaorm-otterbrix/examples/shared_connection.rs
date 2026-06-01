//! Sharing a single Otterbrix-backed `DatabaseConnection` across tasks.
//!
//! The proxy returned by [`OtterbrixProxy::new`] (via the `Arc<Box<dyn
//! ProxyDatabaseTrait>>` it is wrapped in) is `Clone + Send + Sync`. Inside
//! the proxy, the underlying [`otterbrix::Database`] sits behind an internal
//! mutex, so concurrent queries from multiple tokio tasks are serialised
//! safely without any explicit locking on the caller side.
//!
//! Sharing is the simplest correct way to use the proxy from a multi-threaded
//! tokio runtime: clone the `DatabaseConnection`, move the clone into a
//! task, and treat each clone like a normal `&DatabaseConnection`.
//!
//! Run with `cargo run --example shared_connection`.
//!
//! [`OtterbrixProxy::new`]: seaorm_otterbrix::OtterbrixProxy::new

use std::path::Path;
use std::sync::Arc;

use otterbrix::{Config, Database};
use sea_orm::sea_query::Value as SeaValue;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, FromQueryResult, ProxyDatabaseTrait,
    Statement,
};
use seaorm_otterbrix::OtterbrixProxy;

#[derive(Debug, FromQueryResult)]
struct Event {
    id: i64,
    source: String,
}

const TASKS: i64 = 4;
const PER_TASK: i64 = 25;

#[allow(dead_code)] // included by tests/examples_run.rs via #[path]; main is then unused.
fn main() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(err) = rt.block_on(run(tmp.path())) {
        eprintln!("example failed: {err}");
        std::process::exit(1);
    }
}

pub async fn run(data_dir: &Path) -> Result<(), DbErr> {
    let conn = open_audit(data_dir).await?;

    let mut handles = Vec::new();
    for tid in 0..TASKS {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let id = tid * PER_TASK + i;
                let stmt = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "INSERT INTO audit.events (id, source) VALUES ($1, $2);",
                    vec![
                        SeaValue::BigInt(Some(id)),
                        SeaValue::String(Some(Box::new(format!("task-{tid}")))),
                    ],
                );
                conn.execute(stmt).await.expect("insert");
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }

    let totals = conn
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT id, source FROM audit.events;".to_string(),
        ))
        .await?;
    let mut events: Vec<Event> = totals
        .iter()
        .map(|r| Event::from_query_result(r, "").expect("decode"))
        .collect();
    events.sort_by_key(|e| e.id);

    let mut per_source = std::collections::BTreeMap::<String, usize>::new();
    for e in &events {
        *per_source.entry(e.source.clone()).or_default() += 1;
    }

    println!(
        "ingested {} events from {} concurrent task(s):",
        events.len(),
        TASKS
    );
    for (source, count) in per_source {
        println!("  {source:<10}  {count:>3} event(s)");
    }

    assert_eq!(events.len() as i64, TASKS * PER_TASK);
    Ok(())
}

async fn open_audit(data_dir: &Path) -> Result<DatabaseConnection, DbErr> {
    let db = Database::open(Config::new(data_dir))
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;
    db.create_database("audit")
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;
    db.create_collection("audit", "events")
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy).await
}
