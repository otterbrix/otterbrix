use otterbrix::{Error, TransactionStatus};

#[test]
fn display_engine_error() {
    let msg = Error::Engine {
        code: 42,
        message: "boom".to_string(),
        transaction_status: None,
        position: None,
    }
    .to_string();
    assert!(msg.contains("core error"), "missing prefix: {msg:?}");
    assert!(msg.contains("42"), "missing code: {msg:?}");
    assert!(msg.contains("boom"), "missing message: {msg:?}");
    assert!(!msg.contains("transaction"), "no status to show: {msg:?}");
    assert!(!msg.contains("position"), "no position to show: {msg:?}");
}

#[test]
fn display_engine_error_shows_the_transaction_status() {
    let msg = Error::Engine {
        code: 42,
        message: "boom".to_string(),
        transaction_status: Some(TransactionStatus::Failed),
        position: None,
    }
    .to_string();
    assert_eq!(
        msg,
        "otterbrix core error (code 42, transaction failed): boom"
    );
}

#[test]
fn display_engine_error_shows_the_position() {
    let msg = Error::Engine {
        code: 30,
        message: "syntax error at or near \"SELEC\"".to_string(),
        transaction_status: Some(TransactionStatus::Idle),
        position: Some(1),
    }
    .to_string();
    assert_eq!(
        msg,
        "otterbrix core error (code 30, transaction idle, position 1): syntax error at or near \"SELEC\""
    );

    let msg = Error::Engine {
        code: 30,
        message: "boom".to_string(),
        transaction_status: None,
        position: Some(5),
    }
    .to_string();
    assert_eq!(msg, "otterbrix core error (code 30, position 5): boom");
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
    assert_std_error(&Error::InvalidPath(String::new()));
}
