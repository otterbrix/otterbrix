use otterbrix_sys::*;

fn make_sv(s: &str) -> string_view_t {
    string_view_t {
        data: s.as_ptr() as *const i8,
        size: s.len(),
    }
}

#[test]
fn test_create_destroy() {
    unsafe {
        let cfg = config_t {
            level: 0,
            log_path: make_sv("/tmp/otterbrix_rust_test/log"),
            wal_path: make_sv("/tmp/otterbrix_rust_test/wal"),
            disk_path: make_sv("/tmp/otterbrix_rust_test/disk"),
            main_path: make_sv("/tmp/otterbrix_rust_test/main"),
            wal_on: false,
            disk_on: false,
            sync_to_disk: false,
        };

        let db = otterbrix_create(cfg);
        assert!(!db.is_null(), "otterbrix_create returned null");

        otterbrix_destroy(db);
    }
}

#[test]
fn test_execute_sql() {
    unsafe {
        let cfg = config_t {
            level: 0,
            log_path: make_sv("/tmp/otterbrix_rust_test2/log"),
            wal_path: make_sv("/tmp/otterbrix_rust_test2/wal"),
            disk_path: make_sv("/tmp/otterbrix_rust_test2/disk"),
            main_path: make_sv("/tmp/otterbrix_rust_test2/main"),
            wal_on: false,
            disk_on: false,
            sync_to_disk: false,
        };

        let db = otterbrix_create(cfg);
        assert!(!db.is_null());

        let cursor = execute_sql(db, make_sv("CREATE DATABASE test_db;"));
        assert!(!cursor.is_null());
        assert!(cursor_is_success(cursor));
        release_cursor(cursor);

        let cursor = execute_sql(db, make_sv("CREATE TABLE test_db.users();"));
        assert!(!cursor.is_null());
        assert!(cursor_is_success(cursor));
        release_cursor(cursor);

        let cursor = execute_sql(
            db,
            make_sv("INSERT INTO test_db.users (name, age) VALUES ('Alice', 30), ('Bob', 25);"),
        );
        assert!(!cursor.is_null());
        assert!(cursor_is_success(cursor));
        release_cursor(cursor);

        let cursor = execute_sql(db, make_sv("SELECT * FROM test_db.users;"));
        assert!(!cursor.is_null());
        assert!(cursor_is_success(cursor));
        assert_eq!(cursor_size(cursor), 2);
        release_cursor(cursor);

        otterbrix_destroy(db);
    }
}
