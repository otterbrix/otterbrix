#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Raw FFI bindings to the Otterbrix C API.
//!
//! This crate exposes the low-level C functions defined in
//! `integration/c/otterbrix.h` of the upstream Otterbrix project. The bindings
//! are generated at build time by [`bindgen`](https://docs.rs/bindgen) and
//! linked against the shared library `libotterbrix.so`.
//!
//! # When to use this crate
//!
//! `otterbrix-sys` is intended for crate authors who need direct access to the
//! C ABI — for example, building an alternative Rust wrapper or porting another
//! language binding. **For application code, use the safe wrapper crate
//! [`otterbrix`](https://docs.rs/otterbrix) instead**: it provides idiomatic
//! Rust types, owned resources with `Drop`, error handling through `Result`,
//! and a stable surface that does not change with every C header revision.
//!
//! # Build requirements
//!
//! `build.rs` requires two environment variables to be set at compile time:
//!
//! - `OTTERBRIX_LIB_DIR` — directory containing `libotterbrix.so`
//! - `OTTERBRIX_INCLUDE_DIR` — directory containing `otterbrix.h`
//!
//! At link time, `cargo` will pass `-L $OTTERBRIX_LIB_DIR -l otterbrix` to the
//! linker. At run time, the dynamic linker must be able to find
//! `libotterbrix.so`; this is typically arranged through `LD_LIBRARY_PATH`,
//! `rpath`, or by installing the library into a system location.
//!
//! # Safety
//!
//! Every function and type re-exported from [`bindings`] is `unsafe`. Callers
//! are responsible for upholding the C-side contracts:
//!
//! - lifetimes of pointers returned by the engine (cursors must not outlive
//!   the database that produced them; returned strings must be released with
//!   the matching `otterbrix_free_*` function);
//! - thread safety (the C facade serialises every call internally, so concurrent
//!   access to a single `otterbrix_t*` is safe; concurrent access to cursors
//!   from multiple threads is not);
//! - correct memory ownership (the engine owns most returned memory; never
//!   call `free` on returned pointers).
//!
//! Violating these contracts is undefined behaviour. The safe wrapper crate
//! enforces them through Rust's borrow checker.
//!
//! # Stability
//!
//! Not stable. The bindings track the upstream C header verbatim; any change
//! to `otterbrix.h` is a breaking change for this crate.

/// Auto-generated bindings produced by [`bindgen`](https://docs.rs/bindgen)
/// from `integration/c/otterbrix.h`.
///
/// All items in this module are re-exported at the crate root for convenience.
/// Refer to the upstream C header for the canonical contract of each function
/// and type.
#[allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    missing_docs,
    clippy::all
)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::*;
