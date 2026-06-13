use otterbrix_sys::*;
use std::process;

fn make_sv(s: &str) -> string_view_t {
    string_view_t {
        data: s.as_ptr() as *const i8,
        size: s.len(),
    }
}

fn unique_base(tag: &str) -> String {
    format!("/tmp/otterbrix_sys_{tag}_{}", process::id())
}

fn make_config(base: &str) -> (config_t, String, String, String, String) {
    let log_path = format!("{base}/log");
    let wal_path = format!("{base}/wal");
    let disk_path = format!("{base}/disk");
    let main_path = format!("{base}/main");
    let cfg = config_t {
        level: 0,
        log_path: make_sv(&log_path),
        wal_path: make_sv(&wal_path),
        disk_path: make_sv(&disk_path),
        main_path: make_sv(&main_path),
        wal_on: false,
        disk_on: false,
        sync_to_disk: false,
    };
    (cfg, log_path, wal_path, disk_path, main_path)
}

#[test]
fn test_create_destroy() {
    unsafe {
        let base = unique_base("create_destroy");
        let (cfg, _log, _wal, _disk, _main) = make_config(&base);

        let db = otterbrix_create(cfg);
        assert!(!db.is_null(), "otterbrix_create returned null");

        otterbrix_destroy(db);
    }
}

#[test]
fn test_execute_sql() {
    unsafe {
        let base = unique_base("execute_sql");
        let (cfg, _log, _wal, _disk, _main) = make_config(&base);

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
