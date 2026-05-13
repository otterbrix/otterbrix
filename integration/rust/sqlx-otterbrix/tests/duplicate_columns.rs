mod common;

use common::{create_app_db, open_test_conn};
use sqlx::Row;
use sqlx_otterbrix::Otterbrix;

#[tokio::test]
async fn join_with_duplicate_id_uses_positional_keys() -> Result<(), sqlx::Error> {
    let mut t = open_test_conn().await;
    create_app_db(&mut t.conn).await;

    sqlx::query::<Otterbrix>("CREATE TABLE app.t1 (id bigint, val bigint);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("CREATE TABLE app.t2 (id bigint, label string);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.t1 (id, val) VALUES (1, 10), (2, 20);")
        .execute(&mut t.conn)
        .await?;
    sqlx::query::<Otterbrix>("INSERT INTO app.t2 (id, label) VALUES (1, 'a'), (2, 'b');")
        .execute(&mut t.conn)
        .await?;

    let rows = sqlx::query::<Otterbrix>(
        "SELECT t1.id, t2.id FROM app.t1 INNER JOIN app.t2 ON t1.id = t2.id ORDER BY t1.id ASC;",
    )
    .fetch_all(&mut t.conn)
    .await?;
    assert_eq!(rows.len(), 2);

    let r0_a: i64 = rows[0].try_get("00000000")?;
    let r0_b: i64 = rows[0].try_get("00000001")?;
    assert_eq!(r0_a, r0_b);

    let r1_a: i64 = rows[1].try_get("00000000")?;
    let r1_b: i64 = rows[1].try_get("00000001")?;
    assert_eq!(r1_a, r1_b);

    assert_ne!(r0_a, r1_a);

    let by_name = rows[0].try_get::<i64, _>("id");
    assert!(
        by_name.is_err(),
        "duplicate name 'id' must not be resolvable"
    );

    Ok(())
}
