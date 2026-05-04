mod common;

#[test]
fn insert_and_select_single_row() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Alice', 30);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 1);
    assert_eq!(cursor.column_count(), 2);
}

#[test]
fn insert_multiple_rows() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 3);
}

#[test]
fn select_with_where() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, count) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t WHERE count > 15;").unwrap();
    assert_eq!(cursor.size(), 2);
}

#[test]
fn update_rows() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    db.execute("UPDATE db.t SET score = 100 WHERE score < 15;")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t WHERE score = 100;").unwrap();
    assert_eq!(cursor.size(), 1);
    let name: String = cursor.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "a");
}

#[test]
fn delete_rows() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, score) VALUES ('a', 10), ('b', 20), ('c', 30);")
        .unwrap();

    db.execute("DELETE FROM db.t WHERE score > 15;").unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 1);
    let name: String = cursor.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "a");
}
