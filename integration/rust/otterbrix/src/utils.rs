use std::ffi::{c_char, CStr};

pub(crate) fn make_sv(s: &str) -> otterbrix_sys::string_view_t {
    otterbrix_sys::string_view_t {
        data: s.as_ptr() as *const c_char,
        size: s.len(),
    }
}

/// Copies a string a cursor or a value lends; the handle keeps it.
pub(crate) unsafe fn string_from_c(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}

/// Copies and frees the one string the caller owns: otterbrix_create's refusal.
pub(crate) unsafe fn take_string_from_c(ptr: *mut c_char) -> String {
    let s = string_from_c(ptr);
    if !ptr.is_null() {
        otterbrix_sys::otterbrix_free_string(ptr);
    }
    s
}
