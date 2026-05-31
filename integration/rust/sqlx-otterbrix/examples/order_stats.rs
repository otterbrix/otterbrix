//! Aggregated order statistics fetched through SQLx.
//!
//! Demonstrates the `query_as` workflow: a strongly-typed
//! [`FromRow`](sqlx::FromRow) struct decodes the result of a `GROUP BY`
//! aggregation directly into Rust values, with no intermediate `try_get`
//! calls.
//!
//! Inserts a small set of orders, then runs a single grouped query that
//! produces `category, orders, revenue, smallest, largest` per category and
//! prints the result as an aligned table.
//!
//! Run with `cargo run --example order_stats`.

use std::path::Path;

use sqlx::{ConnectOptions, Connection, FromRow};
use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions};

#[derive(Debug, FromRow)]
struct CategoryStats {
    category: String,
    // COUNT() returns an unsigned integer in the engine's logical type
    // system.
    orders: i64,
    revenue: i64,
    smallest: i64,
    largest: i64,
}

#[allow(dead_code)] // included by tests/examples_run.rs via #[path]; main is then unused.
fn main() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(err) = rt.block_on(run(tmp.path())) {
        eprintln!("example failed: {err}");
        std::process::exit(1);
    }
}

pub async fn run(data_dir: &Path) -> Result<(), sqlx::Error> {
    let mut conn = OtterbrixConnectOptions::new(data_dir).connect().await?;

    sqlx::query::<Otterbrix>("CREATE DATABASE shop;")
        .execute(&mut conn)
        .await?;
    sqlx::query::<Otterbrix>(
        "CREATE TABLE shop.orders (id bigint, category string, amount_cents bigint);",
    )
    .execute(&mut conn)
    .await?;

    let orders: &[(i64, &str, i64)] = &[
        (1, "kitchen", 4500),
        (2, "kitchen", 9900),
        (3, "kitchen", 1500),
        (4, "books", 3500),
        (5, "books", 5200),
        (6, "books", 2200),
        (7, "electronics", 2200),
        (8, "electronics", 12800),
        (9, "electronics", 8400),
        (10, "electronics", 3300),
    ];
    for (id, category, amount) in orders {
        sqlx::query::<Otterbrix>(
            "INSERT INTO shop.orders (id, category, amount_cents) VALUES (?, ?, ?);",
        )
        .bind(id)
        .bind(category)
        .bind(amount)
        .execute(&mut conn)
        .await?;
    }

    let stats: Vec<CategoryStats> = sqlx::query_as::<Otterbrix, CategoryStats>(
        "SELECT category, \
                COUNT(id)         AS orders, \
                SUM(amount_cents) AS revenue, \
                MIN(amount_cents) AS smallest, \
                MAX(amount_cents) AS largest \
         FROM shop.orders \
         GROUP BY category \
         ORDER BY revenue DESC;",
    )
    .fetch_all(&mut conn)
    .await?;

    println!(
        "{:<14} {:>7} {:>10} {:>10} {:>10}",
        "category", "orders", "revenue", "smallest", "largest"
    );
    println!("{}", "-".repeat(14 + 1 + 7 + 1 + 10 + 1 + 10 + 1 + 10));
    for s in &stats {
        println!(
            "{:<14} {:>7} {:>10} {:>10} {:>10}",
            s.category, s.orders, s.revenue, s.smallest, s.largest
        );
    }

    let total_revenue: i64 = stats.iter().map(|s| s.revenue).sum();
    let total_orders: i64 = stats.iter().map(|s| s.orders).sum();
    println!("{}", "-".repeat(14 + 1 + 7 + 1 + 10 + 1 + 10 + 1 + 10));
    println!("{:<14} {:>7} {:>10}", "total", total_orders, total_revenue);

    conn.close().await?;
    Ok(())
}
