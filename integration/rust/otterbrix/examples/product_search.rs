//! Parameterised product catalogue search.
//!
//! Demonstrates `execute_with_params`: building a small product catalogue,
//! then issuing two parameterised `SELECT` queries that filter by a numeric
//! range and a string match. Using parameters instead of string-formatting
//! prevents SQL-injection and avoids manual escaping for string literals.
//!
//! Run with `cargo run --example product_search`.

use std::path::Path;

use otterbrix::{Config, Database, Error, SqlParam, SqlParamValue};

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
    db.create_collection("shop", "products")?;

    db.execute(
        "INSERT INTO shop.products (sku, category, name, price_cents, in_stock) VALUES \
         ('K-001', 'kitchen',     'cast iron pan',      4500,  true), \
         ('K-002', 'kitchen',     'chef knife 8\"',    9900,  true), \
         ('K-003', 'kitchen',     'bamboo cutting board', 1500, false), \
         ('B-101', 'books',       'the rust programming language', 3500, true), \
         ('B-102', 'books',       'designing data-intensive apps', 5200, true), \
         ('E-301', 'electronics', 'wireless mouse',     2200,  true), \
         ('E-302', 'electronics', 'mechanical keyboard', 12800, false);",
    )?;

    let min_cents: i64 = 2000;
    let max_cents: i64 = 6000;

    println!("=== products in the {min_cents}..{max_cents} cent range, in stock ===");
    let by_price = db.execute_with_params(
        "SELECT sku, name, price_cents FROM shop.products \
         WHERE price_cents >= $1 AND price_cents <= $2 AND in_stock = true \
         ORDER BY price_cents ASC;",
        &[
            SqlParam {
                index: 1,
                value: SqlParamValue::Int64(min_cents),
            },
            SqlParam {
                index: 2,
                value: SqlParamValue::Int64(max_cents),
            },
        ],
    )?;
    let mut shown = 0;
    for row in by_price.rows() {
        let sku: String = row.get_by_name("sku").get()?;
        let name: String = row.get_by_name("name").get()?;
        let price: i64 = row.get_by_name("price_cents").get()?;
        println!("  {sku} | {price:>6} c | {name}");
        shown += 1;
    }

    let category = "books";
    println!("\n=== category = {category} ===");
    let by_category = db.execute_with_params(
        "SELECT sku, name FROM shop.products WHERE category = $1 ORDER BY sku ASC;",
        &[SqlParam {
            index: 1,
            value: SqlParamValue::Str(category),
        }],
    )?;
    for row in by_category.rows() {
        let sku: String = row.get_by_name("sku").get()?;
        let name: String = row.get_by_name("name").get()?;
        println!("  {sku} | {name}");
    }

    println!("\n{shown} product(s) matched the price filter.");
    Ok(())
}
