//! Runs every program in `examples/` end-to-end against a live engine.
//!
//! The example bodies are exposed as `pub async fn run(data_dir: &Path)`;
//! the tests here import them via `#[path]` and invoke `run` against a
//! unique temporary directory.

#[path = "../examples/book_catalog.rs"]
mod book_catalog;

#[path = "../examples/shared_connection.rs"]
mod shared_connection;

#[tokio::test]
async fn example_book_catalog() {
    let tmp = tempfile::tempdir().expect("tempdir");
    book_catalog::run(tmp.path())
        .await
        .expect("book_catalog example");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn example_shared_connection() {
    let tmp = tempfile::tempdir().expect("tempdir");
    shared_connection::run(tmp.path())
        .await
        .expect("shared_connection example");
}
