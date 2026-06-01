mod common;

use sea_orm::sea_query::Value as SeaValue;
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_through_cloned_connection() {
    let conn = common::open_test_proxy().await;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (-1, 'seed');".to_string(),
    ))
    .await
    .expect("seed");

    const TASKS: i64 = 4;
    const PER_TASK: i64 = 50;

    let mut handles = Vec::new();
    for tid in 0..TASKS {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let id = tid * PER_TASK + i;
                let stmt = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "INSERT INTO app.t (id, name) VALUES ($1, $2);",
                    vec![
                        SeaValue::BigInt(Some(id)),
                        SeaValue::String(Some(Box::new(format!("name-{id}")))),
                    ],
                );
                conn.execute(stmt).await.expect("insert");
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }

    let stmt = Statement::from_string(DbBackend::Postgres, "SELECT id FROM app.t;".to_string());
    let rows = conn.query_all(stmt).await.expect("select all");
    assert_eq!(rows.len() as i64, TASKS * PER_TASK + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_through_cloned_connection() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');".to_string(),
    );
    conn.execute(stmt).await.expect("seed");

    const TASKS: usize = 4;
    const ITERATIONS: usize = 25;

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let stmt = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT id, name FROM app.t WHERE id = $1;",
                    vec![SeaValue::BigInt(Some(2))],
                );
                let rows = conn.query_all(stmt).await.expect("select");
                assert_eq!(rows.len(), 1);

                #[derive(FromQueryResult)]
                struct Row {
                    id: i64,
                    name: String,
                }
                let r = Row::from_query_result(&rows[0], "").expect("decode");
                assert_eq!(r.id, 2);
                assert_eq!(r.name, "beta");
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_writes_and_reads_through_cloned_connection() {
    let conn = common::open_test_proxy().await;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (0, 'seed');".to_string(),
    ))
    .await
    .expect("seed");

    const WRITERS: i64 = 2;
    const READERS: i64 = 2;
    const PER_WRITER: i64 = 50;
    const READS_PER_READER: i64 = 50;

    let mut handles = Vec::new();
    for tid in 0..WRITERS {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                let id = tid * PER_WRITER + i + 1_000;
                let stmt = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "INSERT INTO app.t (id, name) VALUES ($1, $2);",
                    vec![
                        SeaValue::BigInt(Some(id)),
                        SeaValue::String(Some(Box::new(format!("w-{id}")))),
                    ],
                );
                conn.execute(stmt).await.expect("insert");
            }
        }));
    }
    for _ in 0..READERS {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..READS_PER_READER {
                let stmt = Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT id FROM app.t;".to_string(),
                );
                let _rows = conn.query_all(stmt).await.expect("select");
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }

    let stmt = Statement::from_string(DbBackend::Postgres, "SELECT id FROM app.t;".to_string());
    let rows = conn.query_all(stmt).await.expect("select all");
    assert_eq!(rows.len() as i64, WRITERS * PER_WRITER + 1);
}
