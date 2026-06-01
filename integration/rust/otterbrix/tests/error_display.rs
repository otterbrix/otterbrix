use otterbrix::Error;

#[test]
fn display_null_pointer() {
    let msg = Error::NullPointer.to_string();
    assert!(
        msg.contains("null pointer"),
        "unexpected Display for NullPointer: {msg:?}"
    );
}

#[test]
fn display_query_error() {
    let msg = Error::Query {
        code: 42,
        message: "boom".to_string(),
    }
    .to_string();
    assert!(msg.contains("query error"), "missing prefix: {msg:?}");
    assert!(msg.contains("42"), "missing code: {msg:?}");
    assert!(msg.contains("boom"), "missing message: {msg:?}");
}

#[test]
fn display_invalid_path() {
    let msg = Error::InvalidPath("/no/such/path".to_string()).to_string();
    assert!(msg.contains("invalid path"), "missing prefix: {msg:?}");
    assert!(msg.contains("/no/such/path"), "missing path: {msg:?}");
}

#[test]
fn display_type_mismatch() {
    let msg = Error::TypeMismatch {
        expected: "i64",
        got: "string",
    }
    .to_string();
    assert!(msg.contains("type mismatch"), "missing prefix: {msg:?}");
    assert!(msg.contains("i64"), "missing expected: {msg:?}");
    assert!(msg.contains("string"), "missing got: {msg:?}");
}

#[test]
fn error_implements_std_error_trait() {
    fn assert_std_error<E: std::error::Error>(_: &E) {}
    assert_std_error(&Error::NullPointer);
}
