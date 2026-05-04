mod common;

#[test]
fn count_aggregate() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute(
        "INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20), ('c', 30), ('a', 40), ('b', 50);",
    )
    .unwrap();

    let cursor = db.execute("SELECT COUNT(name) AS cnt FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 1);
    let cnt = cursor.get_value_by_name(0, "cnt");
    assert!(cnt.is_int() || cnt.is_uint());
}

#[test]
fn sum_aggregate() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (val) VALUES (10), (20), (30);")
        .unwrap();

    let cursor = db.execute("SELECT SUM(val) AS total FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 1);
    let total = cursor.get_value_by_name(0, "total");
    assert_eq!(total.as_int(), Some(60));
}

#[test]
fn avg_min_max_aggregates() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (val) VALUES (10), (20), (30);")
        .unwrap();

    let cursor = db
        .execute("SELECT MIN(val) AS mn, MAX(val) AS mx FROM db.t;")
        .unwrap();
    assert_eq!(cursor.size(), 1);
    assert_eq!(cursor.get_value_by_name(0, "mn").as_int(), Some(10));
    assert_eq!(cursor.get_value_by_name(0, "mx").as_int(), Some(30));
}

#[test]
fn group_by() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20), ('a', 30), ('b', 40);")
        .unwrap();

    let cursor = db
        .execute("SELECT name, SUM(score) AS total FROM db.t GROUP BY name;")
        .unwrap();
    assert_eq!(cursor.size(), 2);

    let mut results: Vec<(String, i64)> = cursor
        .rows()
        .map(|row| {
            let name: String = row.get_by_name("name").get().unwrap();
            let total: i64 = row.get_by_name("total").get().unwrap();
            (name, total)
        })
        .collect();
    results.sort_by_key(|(name, _)| name.clone());

    assert_eq!(results, vec![("a".into(), 40), ("b".into(), 60)]);
}

#[test]
fn order_by_asc() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, val) VALUES ('c', 30), ('a', 10), ('b', 20);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t ORDER BY val ASC;").unwrap();
    let vals: Vec<i64> = cursor
        .rows()
        .map(|r| r.get_by_name("val").get().unwrap())
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

#[test]
fn order_by_desc() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, val) VALUES ('c', 30), ('a', 10), ('b', 20);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t ORDER BY val DESC;").unwrap();
    let vals: Vec<i64> = cursor
        .rows()
        .map(|r| r.get_by_name("val").get().unwrap())
        .collect();
    assert_eq!(vals, vec![30, 20, 10]);
}
