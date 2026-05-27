use otterbrix::{Config, Database};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn open_test_db() -> Database {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = format!("/tmp/otterbrix_safe_test_{}_{id}", process::id());
    let config = Config::new(&dir);
    Database::open(config).expect("failed to open database")
}
