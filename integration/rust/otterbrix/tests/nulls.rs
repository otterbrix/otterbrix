mod common;

#[test]
fn null_from_missing_column() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Bob', 25);")
        .unwrap();
    db.execute("INSERT INTO db.t (name) VALUES ('Alice');")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t;").unwrap();
    assert_eq!(cursor.size(), 2);

    let mut found_null = false;
    for row in cursor.rows() {
        let age = row.get_by_name("age");
        if age.is_null() {
            found_null = true;
        }
    }
    assert!(found_null, "expected at least one NULL age value");
}

#[test]
fn where_is_null() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Bob', 25);")
        .unwrap();
    db.execute("INSERT INTO db.t (name) VALUES ('Alice');")
        .unwrap();

    let cursor = db.execute("SELECT * FROM db.t WHERE age IS NULL;").unwrap();
    assert_eq!(cursor.size(), 1);
    let name: String = cursor.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "Alice");
}

#[test]
fn where_is_not_null() {
    let db = common::open_test_db();
    db.create_database("db").unwrap();
    db.create_collection("db", "t").unwrap();

    db.execute("INSERT INTO db.t (name, age) VALUES ('Bob', 25);")
        .unwrap();
    db.execute("INSERT INTO db.t (name) VALUES ('Alice');")
        .unwrap();

    let cursor = db
        .execute("SELECT * FROM db.t WHERE age IS NOT NULL;")
        .unwrap();
    assert_eq!(cursor.size(), 1);
    let name: String = cursor.get_value_by_name(0, "name").get().unwrap();
    assert_eq!(name, "Bob");
}
