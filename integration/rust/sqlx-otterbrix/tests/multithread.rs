mod common;

use std::sync::Arc;

use sqlx::Row;
use sqlx_otterbrix::Otterbrix;
use tokio::sync::Mutex;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_connections_in_parallel_tasks() {
    let mut handles = Vec::new();
    for tid in 0..4 {
        handles.push(tokio::spawn(async move {
            let mut test = setup_with_table().await;
            for k in 0..50_i64 {
                sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (?, ?);")
                    .bind(tid * 1000 + k)
                    .bind(k * 2)
                    .execute(&mut test.conn)
                    .await
                    .expect("insert");
            }
            let rows = sqlx::query::<Otterbrix>("SELECT k FROM app.t;")
                .fetch_all(&mut test.conn)
                .await
                .expect("fetch_all");
            assert_eq!(rows.len(), 50);
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_connection_through_arc_mutex() {
    let mut test = setup_with_table().await;
    sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (-1, -1);")
        .execute(&mut test.conn)
        .await
        .expect("seed");
    let _dir = test._dir;
    let conn = Arc::new(Mutex::new(test.conn));

    const TASKS: i64 = 4;
    const PER_TASK: i64 = 25;

    let mut handles = Vec::new();
    for tid in 0..TASKS {
        let conn = Arc::clone(&conn);
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let k = tid * PER_TASK + i;
                let mut guard = conn.lock().await;
                sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (?, ?);")
                    .bind(k)
                    .bind(k * 3)
                    .execute(&mut *guard)
                    .await
                    .expect("insert");
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }

    let mut guard = conn.lock().await;
    let rows = sqlx::query::<Otterbrix>("SELECT k FROM app.t;")
        .fetch_all(&mut *guard)
        .await
        .expect("select");
    assert_eq!(rows.len() as i64, TASKS * PER_TASK + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_through_shared_arc_mutex() {
    let mut test = setup_with_table().await;
    sqlx::query::<Otterbrix>(
        "INSERT INTO app.t (k, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);",
    )
    .execute(&mut test.conn)
    .await
    .expect("seed");
    let _dir = test._dir;
    let conn = Arc::new(Mutex::new(test.conn));

    const TASKS: usize = 4;
    const ITERATIONS: usize = 25;

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let conn = Arc::clone(&conn);
        handles.push(tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let mut guard = conn.lock().await;
                let row = sqlx::query::<Otterbrix>("SELECT v FROM app.t WHERE k = ?")
                    .bind(3_i64)
                    .fetch_one(&mut *guard)
                    .await
                    .expect("fetch_one");
                let v: i64 = row.try_get("v").expect("v");
                assert_eq!(v, 30);
            }
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

type TestSetup = common::TestConn;

async fn setup_with_table() -> TestSetup {
    let mut test = common::open_test_conn().await;
    common::create_app_db(&mut test.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut test.conn)
        .await
        .expect("create table");
    test
}
