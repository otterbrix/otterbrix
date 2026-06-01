mod common;

use sqlx::Row;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn bulk_insert_and_full_select() -> Result<(), sqlx::Error> {
    let mut test = common::open_test_conn().await;
    common::create_app_db(&mut test.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut test.conn)
        .await?;

    const ROWS: i64 = 5_000;
    const BATCH: i64 = 500;

    let mut k = 0i64;
    while k < ROWS {
        let end = (k + BATCH).min(ROWS);
        let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
        let mut first = true;
        for i in k..end {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("({i}, {})", i * 4));
        }
        sql.push(';');
        let result = sqlx::query::<Otterbrix>(&sql)
            .execute(&mut test.conn)
            .await?;
        assert_eq!(result.rows_affected(), (end - k) as u64);
        k = end;
    }

    let rows = sqlx::query::<Otterbrix>("SELECT k FROM app.t;")
        .fetch_all(&mut test.conn)
        .await?;
    assert_eq!(rows.len() as i64, ROWS);
    Ok(())
}

#[tokio::test]
async fn large_select_decodes_all_rows() -> Result<(), sqlx::Error> {
    let mut test = common::open_test_conn().await;
    common::create_app_db(&mut test.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut test.conn)
        .await?;

    const ROWS: i64 = 2_000;
    let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
    for k in 0..ROWS {
        if k > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({k}, {})", k * 5));
    }
    sql.push(';');
    sqlx::query::<Otterbrix>(&sql)
        .execute(&mut test.conn)
        .await?;

    let rows = sqlx::query::<Otterbrix>("SELECT k, v FROM app.t;")
        .fetch_all(&mut test.conn)
        .await?;
    assert_eq!(rows.len() as i64, ROWS);

    let mut sum_k: i64 = 0;
    let mut sum_v: i64 = 0;
    for row in &rows {
        sum_k += row.try_get::<i64, _>("k")?;
        sum_v += row.try_get::<i64, _>("v")?;
    }
    assert_eq!(sum_k, (0..ROWS).sum::<i64>());
    assert_eq!(sum_v, (0..ROWS).map(|x| x * 5).sum::<i64>());
    Ok(())
}

#[tokio::test]
async fn many_point_queries() -> Result<(), sqlx::Error> {
    let mut test = common::open_test_conn().await;
    common::create_app_db(&mut test.conn).await;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint, v bigint);")
        .execute(&mut test.conn)
        .await?;

    let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
    for k in 0..200 {
        if k > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({k}, {})", k * 2));
    }
    sql.push(';');
    sqlx::query::<Otterbrix>(&sql)
        .execute(&mut test.conn)
        .await?;

    for _ in 0..500 {
        let row = sqlx::query::<Otterbrix>("SELECT v FROM app.t WHERE k = ?")
            .bind(137_i64)
            .fetch_one(&mut test.conn)
            .await?;
        let v: i64 = row.try_get("v")?;
        assert_eq!(v, 274);
    }
    Ok(())
}

#[tokio::test]
async fn many_open_close_cycles() -> Result<(), sqlx::Error> {
    for _ in 0..30 {
        let mut test = common::open_test_conn().await;
        common::create_app_db(&mut test.conn).await;
        sqlx::query::<Otterbrix>("CREATE TABLE app.t (k bigint);")
            .execute(&mut test.conn)
            .await?;
        for k in 0..20_i64 {
            sqlx::query::<Otterbrix>("INSERT INTO app.t (k) VALUES (?);")
                .bind(k)
                .execute(&mut test.conn)
                .await?;
        }
        let rows = sqlx::query::<Otterbrix>("SELECT k FROM app.t;")
            .fetch_all(&mut test.conn)
            .await?;
        assert_eq!(rows.len(), 20);
    }
    Ok(())
}
