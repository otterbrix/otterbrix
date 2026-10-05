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
    let default_lib_dir = repo_root
        .join("build")
        .join("release")
        .join("integration")
        .join("c");

    let lib_dir =
        env::var("OTTERBRIX_LIB_DIR").unwrap_or_else(|_| default_lib_dir.display().to_string());

    let abs_lib_dir = PathBuf::from(&lib_dir).canonicalize().unwrap_or_else(|_| {
        panic!(
            "OTTERBRIX_LIB_DIR={lib_dir:?} does not point to an existing directory; \
             build the C++ side first (integration/rust/scripts/build-cpp.sh) or set OTTERBRIX_LIB_DIR explicitly"
        )
    });
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", abs_lib_dir.display());
    println!("cargo:rerun-if-env-changed=OTTERBRIX_LIB_DIR");
}
