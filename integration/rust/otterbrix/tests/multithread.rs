mod common;

use std::sync::{Arc, Mutex};
use std::thread;

use common::open_test_db;
use otterbrix::{Database, SqlParam, SqlParamValue};

fn ddl_setup(db: &Database) {
    db.create_database("app").expect("create database");
    db.execute("CREATE TABLE app.t (k bigint, v bigint);")
        .expect("create table");
}

fn count_rows(db: &Database, table: &str) -> i64 {
    let cursor = db
        .execute(&format!("SELECT COUNT(k) AS cnt FROM {table};"))
        .expect("count");
    assert_eq!(cursor.size(), 1);
    let v = cursor.get_value(0, 0);
    v.as_int()
        .or_else(|| v.as_uint().map(|x| x as i64))
        .expect("count is int or uint")
}

#[test]
fn parallel_inserts_through_arc_mutex_yield_full_count() {
    let db = open_test_db();
    ddl_setup(&db);
    db.execute("INSERT INTO app.t (k, v) VALUES (-1, -1);")
        .expect("seed");
    let db = Arc::new(Mutex::new(db));

    const THREADS: i64 = 4;
    const PER_THREAD: i64 = 250;

    let handles: Vec<_> = (0..THREADS)
        .map(|tid| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let k = tid * PER_THREAD + i;
                    let params = [
                        SqlParam {
                            index: 1,
                            value: SqlParamValue::Int64(k),
                        },
                        SqlParam {
                            index: 2,
                            value: SqlParamValue::Int64(k * 10),
                        },
                    ];
                    let guard = db.lock().expect("lock db");
                    guard
                        .execute_with_params("INSERT INTO app.t (k, v) VALUES ($1, $2);", &params)
                        .expect("insert");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread join");
    }

    let guard = db.lock().expect("lock db");
    assert_eq!(count_rows(&guard, "app.t"), THREADS * PER_THREAD + 1);
}

#[test]
fn parallel_selects_through_arc_mutex_dont_corrupt() {
    let db = open_test_db();
    ddl_setup(&db);
    {
        let mut sql = String::from("INSERT INTO app.t (k, v) VALUES ");
        for k in 0..100 {
            if k > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({k}, {})", k * 7));
        }
        sql.push(';');
        db.execute(&sql).expect("seed");
    }
    let db = Arc::new(Mutex::new(db));

    const THREADS: usize = 4;
    const ITERATIONS: usize = 50;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    let guard = db.lock().expect("lock");
                    let cursor = guard
                        .execute("SELECT v FROM app.t WHERE k = 5;")
                        .expect("select");
                    assert_eq!(cursor.size(), 1);
                    let v = cursor.get_value(0, 0);
                    assert_eq!(v.as_int(), Some(35));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread join");
    }
}

#[test]
fn mixed_read_write_workload_through_arc_mutex() {
    let db = open_test_db();
    ddl_setup(&db);
    db.execute("INSERT INTO app.t (k, v) VALUES (-1, -1);")
        .expect("seed");
    let db = Arc::new(Mutex::new(db));

    const WRITERS: i64 = 2;
    const READERS: i64 = 2;
    const PER_WRITER: i64 = 100;
    const READS_PER_READER: i64 = 50;

    let mut handles = Vec::new();
    for tid in 0..WRITERS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..PER_WRITER {
                let k = tid * PER_WRITER + i + 10_000;
                let guard = db.lock().expect("lock");
                guard
                    .execute(&format!(
                        "INSERT INTO app.t (k, v) VALUES ({k}, {});",
                        k * 2
                    ))
                    .expect("insert");
            }
        }));
    }
    for _ in 0..READERS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for _ in 0..READS_PER_READER {
                let guard = db.lock().expect("lock");
                let cursor = guard.execute("SELECT COUNT(k) FROM app.t;").expect("count");
                assert_eq!(cursor.size(), 1);
            }
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }

    let guard = db.lock().expect("lock");
    assert_eq!(count_rows(&guard, "app.t"), WRITERS * PER_WRITER + 1);
}

#[test]
fn multiple_independent_databases_in_parallel_threads() {
    let handles: Vec<_> = (0..4)
        .map(|_| {
            thread::spawn(move || {
                let db = open_test_db();
                db.create_database("app").expect("create database");
                db.execute("CREATE TABLE app.t (k bigint);")
                    .expect("create table");
                for k in 0..50_i64 {
                    db.execute(&format!("INSERT INTO app.t (k) VALUES ({k});"))
                        .expect("insert");
                }
                assert_eq!(count_rows(&db, "app.t"), 50);
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread join");
    }
}
