mod common;

use common::{create_app_db, open_test_conn};
use sqlx_otterbrix::Otterbrix;

async fn create_table(t: &mut common::TestConn) {
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_reports_inserted_count() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    create_table(&mut t).await;

    let res =
        sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (1, 10), (2, 20), (3, 30);")
            .execute(&mut t.conn)
            .await?;
    assert_eq!(res.rows_affected(), 3);
    Ok(())
}

#[tokio::test]
async fn update_reports_updated_count() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    create_table(&mut t).await;

    sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (1, 10), (2, 20);")
        .execute(&mut t.conn)
        .await?;

    let res = sqlx::query::<Otterbrix>("UPDATE app.t SET v = 99 WHERE k = ?;")
        .bind(1_i64)
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 1);

    let res = sqlx::query::<Otterbrix>("UPDATE app.t SET v = 0;")
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 2);
    Ok(())
}

#[tokio::test]
async fn delete_reports_deleted_count() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    create_table(&mut t).await;

    sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (1, 10), (2, 20), (3, 30);")
        .execute(&mut t.conn)
        .await?;

    let res = sqlx::query::<Otterbrix>("DELETE FROM app.t WHERE v > ?;")
        .bind(15_i64)
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 2);
    Ok(())
}

#[tokio::test]
async fn ddl_reports_zero_rows() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    let res = sqlx::query::<Otterbrix>("CREATE TABLE app.zerotbl (k bigint);")
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 0);
    Ok(())
}

#[tokio::test]
async fn last_insert_id_is_zero_for_inserts() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;
    create_table(&mut t).await;

    let res = sqlx::query::<Otterbrix>("INSERT INTO app.t (k, v) VALUES (1, 10);")
        .execute(&mut t.conn)
        .await?;
    assert_eq!(res.rows_affected(), 1);
    assert_eq!(res.last_insert_rowid(), 0);
    Ok(())
}
