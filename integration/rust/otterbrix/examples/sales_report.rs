//! Aggregated sales report.
//!
//! Demonstrates `GROUP BY` together with `COUNT`, `SUM`, `MIN` and `MAX`:
//! a small `orders` collection is populated with a handful of orders across
//! a few categories, and a single grouped query produces a per-category
//! summary that is then rendered as an aligned table.
//!
//! Run with `cargo run --example sales_report`.

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

pub fn run(data_dir: &Path) -> Result<(), Error> {
    let db = Database::open(Config::new(data_dir))?;
    db.create_database("shop")?;
    db.create_collection("shop", "orders")?;

    db.execute(
        "INSERT INTO shop.orders (id, category, amount_cents) VALUES \
         (1,  'kitchen',     4500), \
         (2,  'kitchen',     9900), \
         (3,  'kitchen',     1500), \
         (4,  'books',       3500), \
         (5,  'books',       5200), \
         (6,  'books',       2200), \
         (7,  'electronics', 2200), \
         (8,  'electronics', 12800), \
         (9,  'electronics', 8400), \
         (10, 'electronics', 3300);",
    )?;

    let report = db.execute(
        "SELECT category, \
                COUNT(id)         AS orders, \
                SUM(amount_cents) AS revenue, \
                MIN(amount_cents) AS smallest, \
                MAX(amount_cents) AS largest \
         FROM shop.orders \
         GROUP BY category \
         ORDER BY revenue DESC;",
    )?;

    println!(
        "{:<14} {:>7} {:>10} {:>10} {:>10}",
        "category", "orders", "revenue", "smallest", "largest"
    );
    println!("{}", "-".repeat(14 + 1 + 7 + 1 + 10 + 1 + 10 + 1 + 10));

    let mut total_orders: u64 = 0;
    let mut total_revenue: i64 = 0;
    for row in report.rows() {
        let category: String = row.get_by_name("category").get()?;
        // COUNT() returns an unsigned integer, while SUM/MIN/MAX inherit the
        // numeric kind of their source column; pick the matching Rust type
        // for each aggregate.
        let orders: u64 = row.get_by_name("orders").get()?;
        let revenue: i64 = row.get_by_name("revenue").get()?;
        let smallest: i64 = row.get_by_name("smallest").get()?;
        let largest: i64 = row.get_by_name("largest").get()?;
        println!("{category:<14} {orders:>7} {revenue:>10} {smallest:>10} {largest:>10}");
        total_orders += orders;
        total_revenue += revenue;
    }

    println!("{}", "-".repeat(14 + 1 + 7 + 1 + 10 + 1 + 10 + 1 + 10));
    println!("{:<14} {:>7} {:>10}", "total", total_orders, total_revenue);

    Ok(())
}
