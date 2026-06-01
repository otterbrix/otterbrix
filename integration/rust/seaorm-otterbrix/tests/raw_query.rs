mod common;

use sea_orm::{
    sea_query::Value as SeaValue, ConnectionTrait, DbBackend, FromQueryResult, Statement,
};

#[tokio::test]
async fn execute_insert_with_no_params() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES (1, 'alpha');".to_string(),
    );
    conn.execute(stmt).await.expect("insert");
}

#[tokio::test]
async fn execute_insert_with_positional_params() {
    let conn = common::open_test_proxy().await;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES ($1, $2);",
        vec![
            SeaValue::BigInt(Some(42)),
            SeaValue::String(Some(Box::new("life".to_string()))),
        ],
    );
    conn.execute(stmt).await.expect("insert");

    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT id, name FROM app.t;".to_string(),
    );
    let rows = conn.query_all(stmt).await.expect("select");
    assert_eq!(rows.len(), 1);

    #[derive(Debug, FromQueryResult)]
    struct Row {
        id: i64,
        name: String,
    }
    let row = Row::from_query_result(&rows[0], "").expect("decode");
    assert_eq!(row.id, 42);
    assert_eq!(row.name, "life");
}

#[tokio::test]
async fn select_where_uses_params_safely() {
    let conn = common::open_test_proxy().await;

    for (id, name) in &[(1i64, "alice"), (2, "bob"), (3, "carol")] {
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO app.t (id, name) VALUES ($1, $2);",
            vec![
                SeaValue::BigInt(Some(*id)),
                SeaValue::String(Some(Box::new((*name).to_string()))),
            ],
        );
        conn.execute(stmt).await.expect("insert");
    }

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT id, name FROM app.t WHERE id = $1;",
        vec![SeaValue::BigInt(Some(2))],
    );
    let rows = conn.query_all(stmt).await.expect("select");
    assert_eq!(rows.len(), 1);

    #[derive(Debug, FromQueryResult)]
    struct Row {
        id: i64,
        name: String,
    }
    let row = Row::from_query_result(&rows[0], "").unwrap();
    assert_eq!(row.id, 2);
    assert_eq!(row.name, "bob");
}

#[tokio::test]
async fn injection_attempt_is_stored_verbatim() {
    let conn = common::open_test_proxy().await;

    let nasty = "x'; DROP TABLE app.t; --";
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO app.t (id, name) VALUES ($1, $2);",
        vec![
            SeaValue::BigInt(Some(1)),
            SeaValue::String(Some(Box::new(nasty.to_string()))),
        ],
    );
    conn.execute(stmt).await.expect("insert");

    let stmt = Statement::from_string(DbBackend::Postgres, "SELECT name FROM app.t;".to_string());
    let rows = conn.query_all(stmt).await.expect("select");
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].try_get("", "name").expect("name");
    assert_eq!(name, nasty);
}
