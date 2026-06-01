# otterbrix

Safe, idiomatic Rust bindings to the [Otterbrix](https://github.com/otterbrix/otterbrix)
in-process database engine.

`otterbrix` is a thin wrapper around the C ABI exposed by the upstream
project. It hides every `unsafe` FFI call behind a small set of owned types
(`Database`, `Cursor`, `Value`) that map cleanly onto Rust's
resource-management model: resources are released by `Drop`, errors come
back as `Result<T, Error>`, and lifetimes prevent use-after-free at compile
time.

For applications that prefer a SQL toolkit or an ORM, two adapters are
built on top of this crate:

- [`sqlx-otterbrix`](../sqlx-otterbrix) — a native `sqlx` driver;
- [`seaorm-otterbrix`](../seaorm-otterbrix) — a SeaORM `ProxyDatabaseTrait`
  adapter.

For very low-level use see [`otterbrix-sys`](../otterbrix-sys).

## Documentation

- API reference: `cargo doc -p otterbrix --open`
- Crate-level overview, error model and concurrency guarantees: see
  the rustdoc on `lib.rs` (`#[doc]` at the top of the crate).

## Examples

The `examples/` directory contains runnable, end-to-end programs that show
typical usage patterns. Each file is also exercised by
`tests/examples_run.rs`, so the published examples are guaranteed to keep
working as the API evolves.

- [`examples/task_journal.rs`](examples/task_journal.rs) — minimal CRUD: open
  a database, insert a few rows, run a `SELECT` with `WHERE` / `ORDER BY`.
- [`examples/product_search.rs`](examples/product_search.rs) — parameterised
  queries via `execute_with_params` with `$N` placeholders; numeric range
  and string equality filters.
- [`examples/sales_report.rs`](examples/sales_report.rs) — `GROUP BY`
  aggregation (`COUNT`, `SUM`, `MIN`, `MAX`) rendered as an aligned table.

Run any of them with:

```bash
cargo run -p otterbrix --example <name>
```

## Build requirements

This crate links against `libotterbrix.so` from the upstream Otterbrix C++
build. The default search path resolved by `otterbrix-sys/build.rs` is
`<repo>/build/integration/c`; both `OTTERBRIX_LIB_DIR` and
`OTTERBRIX_INCLUDE_DIR` may be set to override it.

For the canonical build flow (build the `.so`, run tests, run checks)
see the workspace [README](../README.md) and the helper scripts under
[`integration/rust/scripts/`](../scripts/).

## License

BSD-3-Clause. See the [`LICENSE`](../../../LICENSE) file at the repository
root.
