# seaorm-otterbrix

[SeaORM](https://www.sea-ql.org/SeaORM/) proxy backend for the
[Otterbrix](https://github.com/otterbrix/otterbrix) engine.

The crate plugs into SeaORM through `sea_orm::ProxyDatabaseTrait`: every
query that goes through SeaORM's `DatabaseConnection` is forwarded to an
embedded [`otterbrix::Database`](../otterbrix) instance. The whole crate
is intentionally small — it only does the type-and-error mapping needed to
make Otterbrix look like any other SeaORM-compatible database.

The single public type is `OtterbrixProxy`. All conversion helpers live in
`pub(crate)` modules; the one helper that *is* exposed is
`positional_proxy_column_key`, used to retrieve cells by position when a
result set has duplicate column names.

## Documentation

- API reference: `cargo doc -p seaorm-otterbrix --open`
- Crate-level overview, error mapping and concurrency model: see the
  rustdoc on `lib.rs`.

## Examples

The `examples/` directory contains runnable end-to-end programs; each one
is also exercised by `tests/examples_run.rs` so the published examples
cannot drift out of sync with the API.

- [`examples/book_catalog.rs`](examples/book_catalog.rs) — typical SeaORM
  workflow: `Statement::from_string` for DDL/DML, parameterised
  `Statement::from_sql_and_values` for filtering, decoding rows into a
  `#[derive(FromQueryResult)]` struct.
- [`examples/shared_connection.rs`](examples/shared_connection.rs) —
  cloning the `DatabaseConnection` across multiple tokio tasks. Shows that
  the proxy is `Clone + Send + Sync`, with concurrent queries serialised
  by the proxy's internal mutex.

Run any of them with:

```bash
cargo run -p seaorm-otterbrix --example <name>
```

## Limitations

- **Transactions are not supported.** `begin`, `commit`, `rollback` log a
  warning to the `seaorm_otterbrix` target and return without touching the
  engine.
- **`last_insert_id` is always `0`.** The engine does not surface
  generated identifiers; only `rows_affected` is populated.

See the rustdoc on `lib.rs` for the full list of caveats.

## Build requirements

This crate transitively links against `libotterbrix.so`. The default
search path is `<repo>/build/integration/c`; both `OTTERBRIX_LIB_DIR` and
`OTTERBRIX_INCLUDE_DIR` may be set to override it.

For the canonical build flow (build the `.so`, run tests, run checks)
see the workspace [README](../README.md) and the helper scripts under
[`integration/rust/scripts/`](../scripts/).

## License

BSD-3-Clause. See the [`LICENSE`](../../../LICENSE) file at the repository
root.
