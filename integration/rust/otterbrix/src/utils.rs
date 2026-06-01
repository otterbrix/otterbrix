use std::ffi::{c_char, CStr};

pub(crate) fn make_sv(s: &str) -> otterbrix_sys::string_view_t {
    otterbrix_sys::string_view_t {
        data: s.as_ptr() as *const c_char,
        size: s.len(),
    }
}

pub(crate) unsafe fn string_from_c(ptr: *mut c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    otterbrix_sys::otterbrix_free_string(ptr);
    s
}
