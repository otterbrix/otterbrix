//! Personal task journal: a small CRUD example built on the safe wrapper.
//!
//! Demonstrates the basic life cycle: opening a database, creating a logical
//! database and a collection, inserting a few rows, then reading them back
//! both as a full scan and through a `WHERE`/`ORDER BY` query.
//!
//! Run with `cargo run --example task_journal`.

use std::path::Path;

use otterbrix::{Config, Database, Error};

#[allow(dead_code)] // included by tests/examples_run.rs via #[path]; main is then unused.
fn main() {
    let tmp = tempfile::tempdir().expect("tempdir");
    if let Err(err) = run(tmp.path()) {
        eprintln!("example failed: {err}");
        std::process::exit(1);
    }
}

/// Body of the example, factored out so that it can be re-used by an
/// integration test (`tests/examples_run.rs`).
pub fn run(data_dir: &Path) -> Result<(), Error> {
    let db = Database::open(Config::new(data_dir))?;

    db.create_database("tracker")?;
    db.create_collection("tracker", "tasks")?;

    db.execute(
        "INSERT INTO tracker.tasks (id, title, priority, done) VALUES \
         (1, 'write thesis chapter 3', 3, false), \
         (2, 'review pull request #142', 2, true), \
         (3, 'buy groceries', 1, false), \
         (4, 'fix flaky integration test', 3, false), \
         (5, 'reply to supervisor email', 2, true);",
    )?;

    println!("=== all tasks ===");
    let all = db.execute("SELECT id, title, priority, done FROM tracker.tasks;")?;
    for row in all.rows() {
        let id: i64 = row.get_by_name("id").get()?;
        let title: String = row.get_by_name("title").get()?;
        let priority: i64 = row.get_by_name("priority").get()?;
        let done: bool = row.get_by_name("done").get()?;
        let status = if done { "[x]" } else { "[ ]" };
        println!("  {status} #{id} (prio {priority}): {title}");
    }

    println!("\n=== open tasks, highest priority first ===");
    let open = db.execute(
        "SELECT id, title, priority FROM tracker.tasks \
         WHERE done = false ORDER BY priority DESC;",
    )?;
    let open_count = open.size();
    for row in open.rows() {
        let id: i64 = row.get_by_name("id").get()?;
        let title: String = row.get_by_name("title").get()?;
        let priority: i64 = row.get_by_name("priority").get()?;
        println!("  prio {priority} | #{id} {title}");
    }

    println!("\n{open_count} open task(s) in the journal.");
    Ok(())
}
