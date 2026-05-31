# sqlx-otterbrix

Native [SQLx](https://github.com/launchbadge/sqlx) driver for the
[Otterbrix](https://github.com/otterbrix/otterbrix) engine.

The crate implements every trait that `sqlx-core` requires from a database
backend (`Database`, `Connection`, `ConnectOptions`, `Executor`, `Row`,
`Column`, `Statement`, `TypeInfo`, `Value`, `ValueRef`, `Arguments`,
`DatabaseError`) and provides `Type` / `Encode` / `Decode` instances for
the standard scalar Rust types. With those in place the regular
`sqlx::query`, `sqlx::query_as`, `query.bind(...)`, `row.try_get(...)` API
works exactly as for any other SQLx backend.

The driver also registers an `otterbrix://` URL scheme. The path component
is interpreted as the filesystem base directory passed to
`otterbrix::Config::new`.

## Documentation

- API reference: `cargo doc -p sqlx-otterbrix --open`
- Crate-level overview, URL scheme, placeholder syntax and concurrency
  model: see the rustdoc on `lib.rs`.

## Examples

The `examples/` directory contains runnable end-to-end programs; each one
is also exercised by `tests/examples_run.rs` so the published examples
cannot drift out of sync with the API.

- [`examples/feature_flags.rs`](examples/feature_flags.rs) — typical SQLx
  workflow: `OtterbrixConnection::connect_with`, DDL/DML through
  `sqlx::query::<Otterbrix>`, positional `?` placeholders bound through
  `Query::bind`, single-row fetch with `Row::try_get`.
- [`examples/order_stats.rs`](examples/order_stats.rs) —
  `sqlx::query_as` with a `#[derive(FromRow)]` struct for a `GROUP BY`
  aggregation (`COUNT`, `SUM`, `MIN`, `MAX`) rendered as an aligned table.

Run any of them with:

```bash
cargo run -p sqlx-otterbrix --example <name>
```

## Limitations

- **Transactions are not supported.** `begin` / `commit` / `rollback`
  return `Error::Protocol` with a descriptive message.
- **`prepare_with` is a stub.** It returns a statement with only the
  placeholder count — no column metadata, no parameter type list.
- **`describe` is unsupported.** SQLx's offline macros (`sqlx::query!` &
  friends) cannot be used with this driver.
- **`last_insert_rowid` is always `0`.** The engine does not surface
  generated identifiers; only `rows_affected` is populated.
- **Engine error categorisation is coarse-grained.** All engine errors
  come through with `ErrorKind::Other`; downcast `OtterbrixDbError` for
  the raw integer code.

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
