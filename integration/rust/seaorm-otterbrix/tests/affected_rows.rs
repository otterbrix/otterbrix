mod common;

use sea_orm::{sea_query::Value as SeaValue, ConnectionTrait, DbBackend, Statement};

#[tokio::test]
async fn rows_affected_counts_inserted_rows() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES ($1, $2);",
        vec![
            SeaValue::BigInt(Some(1)),
            SeaValue::String(Some(Box::new("a".into()))),
        ],
    );
    let result = conn.execute(stmt).await.expect("insert");
    assert_eq!(result.rows_affected(), 1);
    assert_eq!(result.last_insert_id(), 0);
}

#[tokio::test]
async fn rows_affected_counts_multi_row_insert() {
    let conn = common::open_test_proxy().await;

    let result = conn
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "INSERT INTO app.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');".to_string(),
        ))
        .await
        .expect("multi-row insert");
    assert_eq!(result.rows_affected(), 3);
}

#[tokio::test]
async fn rows_affected_counts_updated_rows() {
    let conn = common::open_test_proxy().await;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');".to_string(),
    ))
    .await
    .expect("seed");

    let result = conn
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "UPDATE app.t SET name = 'X' WHERE id > 1;".to_string(),
        ))
        .await
        .expect("update");
    assert_eq!(result.rows_affected(), 2);
}

#[tokio::test]
async fn rows_affected_counts_deleted_rows() {
    let conn = common::open_test_proxy().await;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');".to_string(),
    ))
    .await
    .expect("seed");

    let result = conn
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "DELETE FROM app.t WHERE id IN (1, 2);".to_string(),
        ))
        .await
        .expect("delete");
    assert_eq!(result.rows_affected(), 2);
}

#[tokio::test]
async fn rows_affected_is_zero_for_ddl() {
    let conn = common::open_test_proxy().await;

    let result = conn
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "CREATE TABLE app.ddl_target (id bigint);".to_string(),
        ))
        .await
        .expect("ddl");
    assert_eq!(result.rows_affected(), 0);
}
