mod common;

use common::{create_app_db, open_test_conn};
use sqlx::Row;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn syntactically_invalid_sql_yields_database_error() {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    let err = sqlx::query::<Otterbrix>("THIS IS NOT VALID SQL")
        .execute(&mut t.conn)
        .await
        .unwrap_err();
    assert!(matches!(err, sqlx::Error::Database(_)), "got {err:?}");
}

#[tokio::test]
async fn select_from_missing_table_yields_database_error() {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    let err = sqlx::query::<Otterbrix>("SELECT * FROM app.does_not_exist;")
        .fetch_all(&mut t.conn)
        .await
        .unwrap_err();
    assert!(matches!(err, sqlx::Error::Database(_)), "got {err:?}");
}

#[tokio::test]
async fn decoding_string_into_i64_yields_column_decode_error() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.s (k bigint, v string);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.s (k, v) VALUES (1, 'oops');")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT v FROM app.s WHERE k = 1;")
        .fetch_one(&mut t.conn)
        .await?;
    let res: Result<i64, _> = row.try_get("v");
    let err = res.expect_err("must fail");
    assert!(
        matches!(err, sqlx::Error::ColumnDecode { .. }),
        "got {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn decoding_null_into_non_optional_yields_column_decode_error() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.n (k bigint, v bigint);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.n (k, v) VALUES (1, 0), (2, NULL);")
        .execute(&mut t.conn)
        .await?;

    let row = sqlx::query::<Otterbrix>("SELECT * FROM app.n WHERE k = 2;")
        .fetch_one(&mut t.conn)
        .await?;
    let res: Result<i64, _> = row.try_get("v");
    let err = res.expect_err("must fail");
    assert!(
        matches!(err, sqlx::Error::ColumnDecode { .. }),
        "got {err:?}"
    );
    Ok(())
}
