mod common;

use common::{create_app_db, open_test_conn};
use sqlx::Connection;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn begin_returns_protocol_error_with_explanatory_message() {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    let err = t.conn.begin().await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("transactions are not supported"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn ping_is_ok_and_close_is_ok() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    t.conn.ping().await?;
    t.conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn flush_is_ok_and_should_flush_is_false() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    t.conn.flush().await?;
    assert!(!t.conn.should_flush());
    Ok(())
}

#[tokio::test]
async fn shrink_buffers_does_not_panic() {
    let mut t = open_test_conn().await;
    t.conn.shrink_buffers();
}

#[tokio::test]
async fn execute_invalid_dml_returns_database_error_then_connection_remains_usable(
) -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.k (k bigint);")
        .execute(&mut t.conn)
        .await?;

    let err = sqlx::query::<Otterbrix>("INSERT INTO app.unknown (k) VALUES (1);")
        .execute(&mut t.conn)
        .await
        .unwrap_err();
    assert!(matches!(err, sqlx::Error::Database(_)), "got {err:?}");

    let res = sqlx::query::<Otterbrix>("INSERT INTO app.k (k) VALUES (42);")
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 1);
    Ok(())
}
