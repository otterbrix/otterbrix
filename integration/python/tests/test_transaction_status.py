import pytest

import otterbrix
from otterbrix import Client, TransactionStatus


def test_a_result_reports_where_its_session_stands(tmp_path):
    connection = otterbrix.connect(str(tmp_path / "result_status_db"))
    try:
        assert connection.execute("SELECT 1;").transaction_status == TransactionStatus.idle
        assert connection.execute("BEGIN;").transaction_status == TransactionStatus.in_transaction
        closed = connection.execute("SELECT 1;")
        closed.close()
        assert closed.transaction_status is None
    finally:
        connection.close()


def test_an_error_carries_where_its_session_stands(tmp_path):
    connection = otterbrix.connect(str(tmp_path / "error_status_db"))
    try:
        with pytest.raises(RuntimeError) as autocommit_error:
            connection.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
        assert autocommit_error.value.transaction_status == TransactionStatus.idle

        connection.execute("SET autocommit = off;")
        with pytest.raises(RuntimeError) as block_error:
            connection.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
        assert block_error.value.transaction_status == TransactionStatus.failed
    finally:
        connection.close()


def test_a_cursor_reports_where_its_session_stands(tmp_path):
    client = Client(str(tmp_path / "cursor_status_db"))
    assert client.execute("SELECT 1;").transaction_status == TransactionStatus.idle
    failed = client.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
    assert failed.is_error()
    assert failed.transaction_status == TransactionStatus.idle
    assert client.execute("BEGIN;").transaction_status == TransactionStatus.in_transaction
