//! Tiny book catalogue accessed through SeaORM's proxy backend.
//!
//! Demonstrates the typical SeaORM workflow on top of `OtterbrixProxy`:
//!
//! 1. open an Otterbrix engine and wrap it in [`OtterbrixProxy`];
//! 2. obtain a `DatabaseConnection` via [`sea_orm::Database::connect_proxy`];
//! 3. issue DDL/DML through `conn.execute(Statement::from_string(...))`;
//! 4. issue parameterised queries through
//!    `Statement::from_sql_and_values(...)`;
//! 5. decode rows into a strongly-typed struct using
//!    [`FromQueryResult`].
//!
//! Run with `cargo run --example book_catalog`.
//!
//! [`FromQueryResult`]: sea_orm::FromQueryResult
//! [`OtterbrixProxy`]: seaorm_otterbrix::OtterbrixProxy

use std::path::Path;
use std::sync::Arc;

use otterbrix::{Config, Database};
use sea_orm::sea_query::Value as SeaValue;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, FromQueryResult, ProxyDatabaseTrait,
    Statement,
};
use seaorm_otterbrix::OtterbrixProxy;

#[derive(Debug, FromQueryResult)]
struct Book {
    id: i64,
    title: String,
    author: String,
    pages: i64,
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

pub async fn run(data_dir: &Path) -> Result<(), DbErr> {
    let conn = open_catalogue(data_dir).await?;

    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        "INSERT INTO library.books (id, title, author, pages) VALUES \
         (1, 'the rust programming language', 'klabnik', 552), \
         (2, 'designing data-intensive applications', 'kleppmann', 616), \
         (3, 'the pragmatic programmer', 'hunt', 320), \
         (4, 'thinking in systems', 'meadows', 240), \
         (5, 'the c++ programming language', 'stroustrup', 1376);"
            .to_string(),
    ))
    .await?;

    println!("=== full catalogue ===");
    let all = conn
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT id, title, author, pages FROM library.books;".to_string(),
        ))
        .await?;
    for raw in &all {
        let book = Book::from_query_result(raw, "")?;
        println!(
            "  #{:>2} {:<40} by {:<12} ({} pp.)",
            book.id, book.title, book.author, book.pages
        );
    }

    let author = "kleppmann";
    println!("\n=== books by {author} ===");
    let by_author = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT id, title, author, pages FROM library.books WHERE author = $1;",
            vec![SeaValue::String(Some(Box::new(author.to_string())))],
        ))
        .await?;
    for raw in &by_author {
        let book = Book::from_query_result(raw, "")?;
        println!("  #{:>2} {} ({} pp.)", book.id, book.title, book.pages);
    }

    println!("\n{} book(s) total in the catalogue.", all.len());
    Ok(())
}

async fn open_catalogue(data_dir: &Path) -> Result<DatabaseConnection, DbErr> {
    let db = Database::open(Config::new(data_dir))
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;
    db.create_database("library")
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;
    db.create_collection("library", "books")
        .map_err(|e| DbErr::Conn(sea_orm::RuntimeErr::Internal(e.to_string())))?;

    let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
    sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy).await
}
