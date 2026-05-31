# otterbrix-sys

Raw `bindgen`-generated FFI bindings for the
[Otterbrix](https://github.com/otterbrix/otterbrix) C ABI.

This crate is the lowest-level building block of the Rust integration: it
re-exports the unmodified output of `bindgen` against the upstream
`otterbrix.h` header. **Most users want the safe wrapper crate
[`otterbrix`](../otterbrix) instead** — `otterbrix-sys` is intended for:

- writing custom safe wrappers with different ergonomics or trade-offs
  than `otterbrix`;
- building bindings for languages other than Rust on top of the same C
  ABI;
- integrating Otterbrix into existing low-level Rust code that is already
  comfortable with `unsafe` FFI.

The bindings are not stable: regenerating against a newer `otterbrix.h`
may add, remove or rename items without notice.

## Documentation

- API reference: `cargo doc -p otterbrix-sys --open`
- Crate-level overview, build requirements and safety invariants: see the
  rustdoc on `lib.rs`.

## Build requirements

`build.rs` invokes `bindgen` against
`<repo>/build/integration/c/otterbrix.h` and links against
`<repo>/build/integration/c/libotterbrix.so`. Both paths are derived from
`CARGO_MANIFEST_DIR` by default; set `OTTERBRIX_INCLUDE_DIR` and/or
`OTTERBRIX_LIB_DIR` to override them.

For the canonical build flow (building `libotterbrix.so` first, then the
Rust workspace) see the workspace [README](../README.md) and the helper
scripts under [`integration/rust/scripts/`](../scripts/).

## License

BSD-3-Clause. See the [`LICENSE`](../../../LICENSE) file at the repository
root.
