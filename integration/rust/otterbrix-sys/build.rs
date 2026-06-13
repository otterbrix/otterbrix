use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let repo_root = manifest_dir
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("failed to canonicalise repository root");

    let default_lib_dir = repo_root.join("build").join("integration").join("c");
    let default_include_dir = repo_root.join("integration").join("c");

    let lib_dir =
        env::var("OTTERBRIX_LIB_DIR").unwrap_or_else(|_| default_lib_dir.display().to_string());
    let include_dir = env::var("OTTERBRIX_INCLUDE_DIR")
        .unwrap_or_else(|_| default_include_dir.display().to_string());

    let abs_lib_dir = PathBuf::from(&lib_dir).canonicalize().unwrap_or_else(|_| {
        panic!(
            "OTTERBRIX_LIB_DIR={lib_dir:?} does not point to an existing directory; \
             build the C++ side first (cmake --build build) or set OTTERBRIX_LIB_DIR explicitly"
        )
    });
    let abs_include_dir = PathBuf::from(&include_dir)
        .canonicalize()
        .unwrap_or_else(|_| {
            panic!(
                "OTTERBRIX_INCLUDE_DIR={include_dir:?} does not point to an existing directory; \
                 set OTTERBRIX_INCLUDE_DIR explicitly"
            )
        });

    println!("cargo:rustc-link-search=native={}", abs_lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=otterbrix");
    println!(
        "cargo:rustc-link-arg-tests=-Wl,-rpath,{}",
        abs_lib_dir.display()
    );

    println!("cargo:lib_dir={}", abs_lib_dir.display());
    println!("cargo:include_dir={}", abs_include_dir.display());

    let header_path = abs_include_dir.join("otterbrix.h");
    println!("cargo:rerun-if-changed={}", header_path.display());
    println!("cargo:rerun-if-env-changed=OTTERBRIX_LIB_DIR");
    println!("cargo:rerun-if-env-changed=OTTERBRIX_INCLUDE_DIR");

    let bindings = bindgen::Builder::default()
        .header(header_path.display().to_string())
        .clang_arg("-xc++")
        .clang_arg("-std=c++20")
        .allowlist_file(r".*otterbrix\.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("failed to generate otterbrix bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("failed to write otterbrix bindings");
}
