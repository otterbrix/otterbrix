//! Runs every program in `examples/` end-to-end against a live engine.
//!
//! The example bodies are exposed as `pub async fn run(data_dir: &Path)`;
//! the tests here import them via `#[path]` and invoke `run` against a
//! unique temporary directory.

#[path = "../examples/feature_flags.rs"]
mod feature_flags;

#[path = "../examples/order_stats.rs"]
mod order_stats;

#[tokio::test]
async fn example_feature_flags() {
    let tmp = tempfile::tempdir().expect("tempdir");
    feature_flags::run(tmp.path())
        .await
        .expect("feature_flags example");
}

#[tokio::test]
async fn example_order_stats() {
    let tmp = tempfile::tempdir().expect("tempdir");
    order_stats::run(tmp.path())
        .await
        .expect("order_stats example");
}
