mod common;

#[test]
fn rows_iterator_count() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (x) VALUES (1), (2), (3), (4), (5);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.rows().len(), 5);
    assert_eq!(cursor.rows().count(), 5);
}

#[test]
fn rows_iterator_values() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, val) VALUES ('a', 1), ('b', 2), ('c', 3);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let names: Vec<String> = cursor
        .rows()
        .map(|row| row.get_by_name("name").as_str().unwrap().to_string())
        .collect();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"a".to_string()));
    assert!(names.contains(&"b".to_string()));
    assert!(names.contains(&"c".to_string()));
}

#[test]
fn row_index() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (x) VALUES (10), (20);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    let indices: Vec<i32> = cursor.rows().map(|r| r.index()).collect();
    assert_eq!(indices, vec![0, 1]);
}
