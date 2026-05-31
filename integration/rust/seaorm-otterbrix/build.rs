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

    let lib_dir =
        env::var("OTTERBRIX_LIB_DIR").unwrap_or_else(|_| default_lib_dir.display().to_string());

    if let Ok(abs) = PathBuf::from(&lib_dir).canonicalize() {
        println!("cargo:rustc-link-arg-tests=-Wl,-rpath,{}", abs.display());
    }
    println!("cargo:rerun-if-env-changed=OTTERBRIX_LIB_DIR");
}
