mod common;

use sea_orm::{sea_query::Value as SeaValue, ConnectionTrait, DbBackend, DbErr, Statement};

#[tokio::test]
async fn null_parameter_returns_type_error() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (id) VALUES ($1);",
        vec![SeaValue::BigInt(None)],
    );
    let err = conn.execute(stmt).await.unwrap_err();
    assert!(matches!(err, DbErr::Type(_)), "got {err:?}");
    let msg = format!("{err}");
    assert!(msg.contains("NULL"), "unexpected error: {msg}");
}

#[tokio::test]
async fn unsupported_parameter_type_returns_type_error() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (data) VALUES ($1);",
        vec![SeaValue::Bytes(Some(Box::new(vec![0u8, 1, 2])))],
    );
    let err = conn.execute(stmt).await.unwrap_err();
    assert!(matches!(err, DbErr::Type(_)), "got {err:?}");
}

#[tokio::test]
async fn invalid_sql_surfaces_exec_error() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_string(DbBackend::Postgres, "this is not sql".to_string());
    let err = conn.execute(stmt).await.unwrap_err();
    assert!(matches!(err, DbErr::Exec(_)), "got {err:?}");
}
