mod common;

use common::{create_app_db, open_test_conn_tweak};
use log::LevelFilter;
use sqlx::Row;
use sqlx_core::connection::ConnectOptions;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn log_statements_setting_does_not_break_queries() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn_tweak(|opts| {
        opts.log_statements(LevelFilter::Trace)
            .log_slow_statements(LevelFilter::Warn, std::time::Duration::from_micros(1))
    })
    .await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (1, 100);")
        .execute(&mut t.conn)
        .await?;
    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.t WHERE k = ?")
        .bind(1_i64)
        .fetch_one(&mut t.conn)
        .await?;
    assert_eq!(row.try_get::<i64, _>("v")?, 100);
    Ok(())
}

#[tokio::test]
async fn log_statements_disable_via_off_filter_still_works() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn_tweak(|opts| opts.disable_statement_logging()).await;
    create_app_db(&mut t.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint);")
        .execute(&mut t.conn)
        .await?;
    Ok(())
}
