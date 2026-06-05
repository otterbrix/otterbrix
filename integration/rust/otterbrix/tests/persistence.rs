use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

use otterbrix::{Config, Database, SqlParam, SqlParamValue};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fresh_persistent_dir() -> String {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = format!("/tmp/otterbrix_persistence_test_{}_{id}", process::id());
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn persistent_config(dir: &str) -> Config {
    Config::builder()
        .log_path(format!("{dir}/log"))
        .wal_path(format!("{dir}/wal"))
        .disk_path(format!("{dir}/disk"))
        .main_path(format!("{dir}/main"))
        .wal_on(true)
        .disk_on(true)
        .sync_to_disk(true)
        .build()
}

#[test]
fn data_persists_across_reopen() {
    let dir = fresh_persistent_dir();
    {
        let db = Database::open(persistent_config(&dir)).expect("first open");
        db.create_database("p").expect("create database");
        db.create_collection("p", "t").expect("create collection");
        db.execute_with_params(
            "INSERT INTO p.t (id, name) VALUES ($1, $2);",
            &[
                SqlParam {
                    index: 1,
                    value: SqlParamValue::Int64(1),
                },
                SqlParam {
                    index: 2,
                    value: SqlParamValue::Str("alice"),
                },
            ],
        )
        .expect("insert");
    }

    {
        let db = Database::open(persistent_config(&dir)).expect("second open on same dir");
        let cur = db
            .execute("SELECT id, name FROM p.t;")
            .expect("select after reopen");
        assert_eq!(cur.size(), 1, "row count must persist across reopen");
        let id: i64 = cur.get_value_by_name(0, "id").get().expect("id");
        let name: String = cur.get_value_by_name(0, "name").get().expect("name");
        assert_eq!(id, 1);
        assert_eq!(name, "alice");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
