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


def test_a_connection_is_one_session(tmp_path):
    connection = otterbrix.connect(str(tmp_path / "one_session_connection_db"))
    try:
        connection.execute("CREATE DATABASE s;")
        connection.execute("CREATE TABLE s.t (a BIGINT);")
        connection.execute("BEGIN;")
        connection.execute("INSERT INTO s.t (a) VALUES (1);")
        connection.execute("ROLLBACK;")
        assert connection.execute("SELECT a FROM s.t;").fetchall() == []

        connection.execute("BEGIN;")
        with pytest.raises(RuntimeError) as block_error:
            connection.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
        assert block_error.value.transaction_status == TransactionStatus.failed
        assert connection.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
        assert connection.execute("SELECT 1;").transaction_status == TransactionStatus.idle

        cursor = connection.cursor()
        assert cursor.execute("BEGIN;").transaction_status == TransactionStatus.in_transaction
        assert connection.execute("SELECT 1;").transaction_status == TransactionStatus.in_transaction
        assert connection.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
    finally:
        connection.close()


def test_a_client_is_one_session(tmp_path):
    client = Client(str(tmp_path / "one_session_client_db"))
    assert client.execute("CREATE DATABASE s;").is_success()
    assert client.execute("CREATE TABLE s.t (a BIGINT);").is_success()
    assert client.execute("BEGIN;").is_success()
    assert client.execute("INSERT INTO s.t (a) VALUES (1);").is_success()
    assert client.execute("ROLLBACK;").is_success()
    assert len(client.execute("SELECT a FROM s.t;")) == 0

    assert client.execute("BEGIN;").is_success()
    failed = client.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
    assert failed.is_error()
    assert failed.transaction_status == TransactionStatus.failed
    assert client.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
    assert client.execute("SELECT 1;").transaction_status == TransactionStatus.idle

    cursor = client.execute("BEGIN;")
    cursor.execute("SELECT 1;")
    assert cursor.transaction_status == TransactionStatus.in_transaction
    assert client.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
