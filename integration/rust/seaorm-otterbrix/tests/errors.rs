mod common;

use sea_orm::{sea_query::Value as SeaValue, ConnectionTrait, DbBackend, DbErr, Statement};

#[tokio::test]
async fn null_bigint_parameter_is_not_rejected_by_adapter() {
    let conn = common::open_test_proxy().await;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "CREATE TABLE app.nullbind (name string, value bigint);".to_string(),
    ))
    .await
    .expect("create table");

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.nullbind (name, value) VALUES ('x', $1);",
        vec![SeaValue::BigInt(None)],
    );
    match conn.execute(stmt).await {
        Ok(_) => {}
        Err(DbErr::Exec(_)) => {}
        Err(e) => panic!("expected success or otterbrix Exec error, not adapter rejection: {e:?}"),
    }
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
