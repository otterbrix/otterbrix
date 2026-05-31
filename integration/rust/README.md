# Rust integration for Otterbrix

This workspace provides a four-layer Rust binding stack for the
[Otterbrix](https://github.com/otterbrix/otterbrix) in-process database
engine.

| Crate | Purpose |
|---|---|
| [`otterbrix-sys`](otterbrix-sys/) | Raw `bindgen` FFI to `libotterbrix.so`. Low-level, `unsafe`. |
| [`otterbrix`](otterbrix/) | Safe, idiomatic Rust wrapper. The recommended entry point for most users. |
| [`seaorm-otterbrix`](seaorm-otterbrix/) | SeaORM `ProxyDatabaseTrait` adapter. |
| [`sqlx-otterbrix`](sqlx-otterbrix/) | Native SQLx driver. |

Every crate has its own `README.md` with a short description, runnable
examples and a list of known limitations. API reference is available
locally with `cargo doc --workspace --open`.

## Layout

```
integration/rust/
├── Cargo.toml             workspace manifest (members + shared MSRV)
├── otterbrix-sys/         FFI bindings (bindgen)
├── otterbrix/             safe wrapper
├── seaorm-otterbrix/      SeaORM proxy backend
├── sqlx-otterbrix/        SQLx driver
└── scripts/
    ├── build-cpp.sh       build libotterbrix.so via Conan + CMake + Ninja
    ├── check.sh           cargo fmt --check + clippy + rustdoc strict
    └── test.sh            cargo test --workspace (lib + integration + doctests)
```

Each crate's `examples/` directory contains runnable end-to-end programs.
The same code is re-used by `tests/examples_run.rs` so the published
examples cannot drift out of sync with the API.

## Build

The Rust crates link against `libotterbrix.so`, produced by the upstream
C++ build. From a clean checkout:

```bash
# 1. Build libotterbrix.so (Conan + CMake + Ninja). Requires conan,
#    cmake and ninja in PATH; outputs build/integration/c/libotterbrix.so.
./integration/rust/scripts/build-cpp.sh

# 2. Run the Rust workspace tests (lib, integration and doctests).
./integration/rust/scripts/test.sh

# 3. Run the static checks (rustfmt, clippy -D warnings, rustdoc strict).
./integration/rust/scripts/check.sh
```

Both `test.sh` and `check.sh` are pure wrappers over `cargo`; if you want
to invoke `cargo` directly, set `RUSTFLAGS` and `RUSTDOCFLAGS` so the
linker embeds an `rpath` for `libotterbrix.so`:

```bash
LIB_DIR="$(pwd)/build/integration/c"
export RUSTFLAGS="-C link-arg=-Wl,-rpath,$LIB_DIR"
export RUSTDOCFLAGS="-C link-arg=-Wl,-rpath,$LIB_DIR"

cd integration/rust
cargo test --workspace
```

By default the build scripts resolve `libotterbrix.so` and `otterbrix.h`
under `<repo>/build/integration/c`. Override with `OTTERBRIX_LIB_DIR`
and/or `OTTERBRIX_INCLUDE_DIR` if you keep the C++ artefacts elsewhere.

## CI / Docker

A reproducible build is provided by
[`docker/Dockerfile-ubuntu-22-rust`](../../docker/Dockerfile-ubuntu-22-rust)
(multi-stage: builds the C++ library, then runs the Rust checks and
tests) and the GitHub Actions workflow at
[`.github/workflows/rust.yaml`](../../.github/workflows/rust.yaml). The
workflow shells out to
[`scripts/CI/run-rust-checks.sh`](../../scripts/CI/run-rust-checks.sh),
which is itself a thin wrapper over `scripts/check.sh` + `scripts/test.sh`.

## Minimum supported Rust version (MSRV)

`1.86.0`. The MSRV is enforced via `rust-version` in the workspace
manifest and pinned in CI. Raising it requires updating both this README
and `Cargo.toml`.

## Contributing

The standard upstream contribution process applies; see
[`CONTRIBUTING.md`](../../CONTRIBUTING.md) and
[`ContributorLicenseAgreement.md`](../../ContributorLicenseAgreement.md)
at the repository root.

## License

BSD-3-Clause. See the [`LICENSE`](../../LICENSE) file at the repository
root.
