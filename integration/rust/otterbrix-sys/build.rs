use std::env;
use std::path::PathBuf;

fn main() {
    let lib_dir = env::var("OTTERBRIX_LIB_DIR")
        .expect("OTTERBRIX_LIB_DIR must be set to the directory containing libotterbrix.so");
    let include_dir = env::var("OTTERBRIX_INCLUDE_DIR")
        .expect("OTTERBRIX_INCLUDE_DIR must be set to the directory containing otterbrix.h");

    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib=dylib=otterbrix");

    println!("cargo:rerun-if-changed={include_dir}/otterbrix.h");
    println!("cargo:rerun-if-env-changed=OTTERBRIX_LIB_DIR");
    println!("cargo:rerun-if-env-changed=OTTERBRIX_INCLUDE_DIR");

    let bindings = bindgen::Builder::default()
        .header(format!("{include_dir}/otterbrix.h"))
        .clang_arg("-xc++")
        .clang_arg("-std=c++20")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("failed to generate otterbrix bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("failed to write otterbrix bindings");
}
