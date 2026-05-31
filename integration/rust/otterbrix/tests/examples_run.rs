//! Runs every program in `examples/` against a live engine, so that
//! drift between the published examples and the real API is caught by
//! `cargo test`.
//!
//! Each example exposes its body as `pub fn run(data_dir: &Path)`; the
//! test imports that file via `#[path]` and calls `run` against a unique
//! temporary directory.

#[path = "../examples/task_journal.rs"]
mod task_journal;

#[path = "../examples/product_search.rs"]
mod product_search;

#[path = "../examples/sales_report.rs"]
mod sales_report;

#[test]
fn example_task_journal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    task_journal::run(tmp.path()).expect("task_journal example");
}

#[test]
fn example_product_search() {
    let tmp = tempfile::tempdir().expect("tempdir");
    product_search::run(tmp.path()).expect("product_search example");
}

#[test]
fn example_sales_report() {
    let tmp = tempfile::tempdir().expect("tempdir");
    sales_report::run(tmp.path()).expect("sales_report example");
}
