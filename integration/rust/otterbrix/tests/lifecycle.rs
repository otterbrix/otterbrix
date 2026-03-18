mod common;

#[test]
fn open_and_drop() {
    let _db = common::open_test_db();
}

#[test]
fn open_multiple_instances() {
    let _a = common::open_test_db();
    let _b = common::open_test_db();
}
