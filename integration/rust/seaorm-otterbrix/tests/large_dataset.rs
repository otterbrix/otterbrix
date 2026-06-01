mod common;

use sea_orm::sea_query::Value as SeaValue;
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement};

#[tokio::test]
async fn bulk_insert_via_proxy_returns_correct_rows_affected() {
    let conn = common::open_test_proxy().await;

    const ROWS: i64 = 5_000;
    const BATCH: i64 = 250;

    let mut id = 0;
    while id < ROWS {
        let end = (id + BATCH).min(ROWS);
        let mut sql = String::from("INSERT INTO app.t (id, name) VALUES ");
        let mut first = true;
        for k in id..end {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("({k}, 'name-{k}')"));
        }
        sql.push(';');
        let result = conn
            .execute(Statement::from_string(DbBackend::Postgres, sql))
            .await
            .expect("batch insert");
        assert_eq!(result.rows_affected(), (end - id) as u64);
        id = end;
    }

    let rows = conn
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT id FROM app.t;".to_string(),
        ))
        .await
        .expect("select all");
    assert_eq!(rows.len() as i64, ROWS);
}

#[tokio::test]
async fn large_select_via_proxy_returns_all_rows() {
    let conn = common::open_test_proxy().await;

    const ROWS: i64 = 2_000;
    let mut sql = String::from("INSERT INTO app.t (id, name) VALUES ");
    for k in 0..ROWS {
        if k > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({k}, 'n-{k}')"));
    }
    sql.push(';');
    conn.execute(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .expect("seed");

    let rows = conn
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT id, name FROM app.t;".to_string(),
        ))
        .await
        .expect("select all");
    assert_eq!(rows.len() as i64, ROWS);

    #[derive(FromQueryResult)]
    struct Row {
        id: i64,
        name: String,
    }
    let mut sum: i64 = 0;
    for r in &rows {
        let row = Row::from_query_result(r, "").expect("decode");
        sum += row.id;
        assert!(row.name.starts_with("n-"));
    }
    assert_eq!(sum, (0..ROWS).sum::<i64>());
}

#[tokio::test]
async fn many_point_queries_via_proxy() {
    let conn = common::open_test_proxy().await;

    let mut sql = String::from("INSERT INTO app.t (id, name) VALUES ");
    for k in 0..200 {
        if k > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({k}, 'name-{k}')"));
    }
    sql.push(';');
    conn.execute(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .expect("seed");

    for _ in 0..500 {
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT id, name FROM app.t WHERE id = $1;",
            vec![SeaValue::BigInt(Some(137))],
        );
        let rows = conn.query_all(stmt).await.expect("point select");
        assert_eq!(rows.len(), 1);
    }
}

#[tokio::test]
async fn many_open_close_cycles_via_proxy() {
    for _ in 0..30 {
        let conn = common::open_test_proxy().await;
        let stmt = Statement::from_string(
            DbBackend::Postgres,
            "INSERT INTO app.t (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');".to_string(),
        );
        let result = conn.execute(stmt).await.expect("insert");
        assert_eq!(result.rows_affected(), 3);

        let rows = conn
            .query_all(Statement::from_string(
                DbBackend::Postgres,
                "SELECT id FROM app.t;".to_string(),
            ))
            .await
            .expect("select");
        assert_eq!(rows.len(), 3);
    }
}
