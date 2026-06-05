mod common;

#[test]
fn database_debug() {
    let db = common::open_test_db();
    let s = format!("{db:?}");
    assert!(s.contains("Database"));
}
