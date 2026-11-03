"""A connection from connect() and a Client own their session, and what they answered holds them: a connect() copy
from cursor() and a Client's cursor. The session closes, rolling back what its transaction did, on close() or when the
last holder goes (docs/embedding-host-api.md, "Sessions")."""

import gc
import subprocess
import sys

import pytest

import otterbrix
from otterbrix import Client, Connection, TransactionStatus

_CLOSE_LINE = "manager_dispatcher_t::close_session session:"


def _closes_in_log(database):
    """How many sessions the engine of `database` closed; read once the engine is gone, its log flushed."""
    text = ""
    for log_file in (database / "log").iterdir():
        text += log_file.read_text(errors="replace")
    return text.count(_CLOSE_LINE)


def _client_with_an_open_insert(database):
    client = Client(str(database))
    assert client.execute("CREATE SCHEMA s;").is_success()
    assert client.execute("CREATE TABLE s.t (a BIGINT);").is_success()
    assert client.execute("BEGIN;").is_success()
    assert client.execute("INSERT INTO s.t (a) VALUES (1);").is_success()
    return client


def _connection_with_an_open_insert(database):
    connection = otterbrix.connect(str(database))
    connection.execute("CREATE SCHEMA s;")
    connection.execute("CREATE TABLE s.t (a BIGINT);")
    connection.execute("BEGIN;")
    connection.execute("INSERT INTO s.t (a) VALUES (1);")
    return connection


def _refused_as_closed(cursor):
    return cursor.is_error() and cursor.get_error()[1] == "the session is closed"


def test_a_cursor_of_a_closed_client_is_refused(tmp_path):
    client = Client(str(tmp_path / "client_cursor_after_close_db"))
    cursor = client.execute("SELECT 1;")
    client.close()
    cursor.execute("SELECT 1;")
    assert _refused_as_closed(cursor)


def test_a_closed_client_is_refused(tmp_path):
    client = Client(str(tmp_path / "client_after_close_db"))
    client.close()
    assert _refused_as_closed(client.execute("SELECT 1;"))


def test_closing_a_client_twice_closes_its_session_once(tmp_path):
    database = tmp_path / "client_close_twice_db"
    client = _client_with_an_open_insert(database)
    client.close()
    client.close()
    del client
    gc.collect()
    assert _closes_in_log(database) == 1


def test_a_client_closes_its_session_when_its_last_cursor_goes(tmp_path):
    database = tmp_path / "client_last_cursor_db"
    client = _client_with_an_open_insert(database)
    cursor = client.execute("SELECT 1;")
    del client
    gc.collect()
    cursor.execute("SELECT a FROM s.t;")
    assert cursor.transaction_status == TransactionStatus.in_transaction
    assert cursor.fetchall() == [(1,)]
    del cursor
    gc.collect()
    assert _closes_in_log(database) == 1


_CLOSE_THROUGH_CONNECTION = """
import sys
from otterbrix import Client, Connection

client = Client(sys.argv[1])
Connection(client).close()
answer = client.execute("SELECT 1;")
assert answer.is_error()
assert answer.get_error()[1] == "the session is closed"
print("SURVIVED")
"""


def test_closing_a_connection_closes_its_client(tmp_path):
    # A null dereference here once took the process down, so it runs in a child.
    proc = subprocess.run(
        [sys.executable, "-c", _CLOSE_THROUGH_CONNECTION, str(tmp_path / "connection_close_db")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "SURVIVED" in proc.stdout, proc.stdout


def test_a_connection_closes_its_session_when_its_last_copy_goes(tmp_path):
    database = tmp_path / "connection_last_copy_db"
    connection = _connection_with_an_open_insert(database)
    cursor = connection.cursor()
    del connection
    gc.collect()
    result = cursor.execute("SELECT a FROM s.t;")
    assert result.transaction_status == TransactionStatus.in_transaction
    assert result.fetchall() == [(1,)]
    del result
    del cursor
    gc.collect()
    assert _closes_in_log(database) == 1


def test_a_copy_of_a_closed_connection_is_refused(tmp_path):
    connection = _connection_with_an_open_insert(tmp_path / "connection_copy_after_close_db")
    cursor = connection.cursor()
    connection.close()
    with pytest.raises(RuntimeError, match="the connection is closed"):
        cursor.execute("SELECT 1;")


def test_closing_a_cursor_leaves_the_connections_transaction_open(tmp_path):
    connection = _connection_with_an_open_insert(tmp_path / "cursor_close_keeps_session_db")
    try:
        connection.cursor().close()
        assert connection.execute("SELECT 1;").transaction_status == TransactionStatus.in_transaction
        assert connection.execute("COMMIT;").transaction_status == TransactionStatus.idle
        assert connection.execute("SELECT a FROM s.t;").fetchall() == [(1,)]
    finally:
        connection.close()


def test_dropping_a_cursor_leaves_the_connections_transaction_open(tmp_path):
    connection = _connection_with_an_open_insert(tmp_path / "cursor_drop_keeps_session_db")
    try:
        cursor = connection.cursor()
        del cursor
        gc.collect()
        assert connection.execute("SELECT 1;").transaction_status == TransactionStatus.in_transaction
        assert connection.execute("COMMIT;").transaction_status == TransactionStatus.idle
    finally:
        connection.close()


def _run_child(body, database, cwd=None):
    """A child interpreter: a closed engine that is still held shows only as a refused reopen, and a dangling pointer
    only as a dead process."""
    return subprocess.run(
        [sys.executable, "-c", body, str(database)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        cwd=cwd,
    )


def _assert_finished(proc):
    assert proc.returncode == 0, "process ended with %s\n%s\n%s" % (proc.returncode, proc.stdout, proc.stderr[-3000:])
    assert "DONE" in proc.stdout, proc.stdout


_CONNECTION_RELEASES_ITS_ENGINE = """
import sys

import pandas as pd

import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

path = sys.argv[1]
con = otterbrix.connect(path)
con.execute("CREATE SCHEMA s;")
con.execute("CREATE TABLE s.t (a BIGINT);")
con.execute("INSERT INTO s.t (a) VALUES (1);")
condition = ColumnExpression("x", con) == ConstantExpression("OH", con)
rel = con.from_df(pd.DataFrame({"s": [{"a": 1}] * 2, "n": [1, 2]}))
types = list(rel.types)
result = con.execute("SELECT a FROM s.t;")
copy = con.cursor()
con.close()

other = otterbrix.connect(path)
assert other.execute("SELECT a FROM s.t;").fetchall() == [(1,)]
other.close()

assert str(condition) == '"x": {$eq: #0}', str(condition)
assert [name for name, _ in types[0].children] == ["a"], types[0].children
for name, read in (
    ("result", result.fetchall),
    ("relation", rel.fetchall),
    ("copy", lambda: copy.execute("SELECT 1;")),
):
    try:
        read()
    except RuntimeError as error:
        assert "the connection is closed" in str(error), (name, str(error))
    else:
        raise AssertionError(name + " was not refused")
print("DONE")
"""


def test_closing_a_connection_releases_its_engine(tmp_path):
    _assert_finished(_run_child(_CONNECTION_RELEASES_ITS_ENGINE, tmp_path / "connection_release_db"))


_CLIENT_RELEASES_ITS_ENGINE = """
import sys

import otterbrix
from otterbrix import Client

path = sys.argv[1]
client = Client(path)
assert client.execute("CREATE SCHEMA s;").is_success()
assert client.execute("CREATE TABLE s.t (a BIGINT);").is_success()
assert client.execute("INSERT INTO s.t (a) VALUES (1);").is_success()
cursor = client.execute("SELECT a FROM s.t;")
client.close()

other = otterbrix.connect(path)
assert other.execute("SELECT a FROM s.t;").fetchall() == [(1,)]
other.close()

assert cursor.fetchall() == [(1,)]
cursor.execute("SELECT 1;")
assert cursor.is_error() and cursor.get_error()[1] == "the session is closed", cursor.get_error()
print("DONE")
"""


def test_closing_a_client_releases_its_engine(tmp_path):
    _assert_finished(_run_child(_CLIENT_RELEASES_ITS_ENGINE, tmp_path / "client_release_db"))


_DEFAULT_CONNECTION_AFTER_CLOSE = """
import otterbrix

closed = otterbrix.connect("default")
closed.close()
reopened = otterbrix.connect("default")
assert reopened is not closed
assert reopened.execute("SELECT 1;").fetchall() == [(1,)]
try:
    closed.execute("SELECT 1;")
except RuntimeError as error:
    assert "the connection is closed" in str(error), str(error)
else:
    raise AssertionError("the closed default connection ran a statement")
reopened.close()
print("DONE")
"""


def test_the_default_connection_reopens_after_close(tmp_path):
    _assert_finished(_run_child(_DEFAULT_CONNECTION_AFTER_CLOSE, "unused", cwd=str(tmp_path)))


_CONNECTION_OF_A_DROPPED_CLIENT = """
import gc
import sys

from otterbrix import Client, Connection

connection = Connection(Client(sys.argv[1]))
gc.collect()
assert connection.execute("SELECT 1;").is_success()
connection.close()
connection.close()
answer = connection.execute("SELECT 1;")
assert answer.is_error() and answer.get_error()[1] == "the session is closed", answer.get_error()
print("DONE")
"""


def test_a_connection_holds_its_client(tmp_path):
    _assert_finished(_run_child(_CONNECTION_OF_A_DROPPED_CLIENT, tmp_path / "connection_holds_client_db"))


def _connection_with_a_table(database):
    client = Client(str(database))
    connection = Connection(client)
    assert connection.execute("CREATE SCHEMA s;").is_success()
    assert connection.execute("CREATE TABLE s.t (a BIGINT);").is_success()
    return client, connection


def test_connection_commit_commits_in_the_client_session(tmp_path):
    database = tmp_path / "connection_commit_db"
    client, connection = _connection_with_a_table(database)
    assert connection.execute("BEGIN;").is_success()
    assert connection.execute("INSERT INTO s.t (a) VALUES (1);").is_success()
    committed = connection.commit()
    assert committed.is_success()
    assert committed.transaction_status == TransactionStatus.idle
    connection.close()
    del connection, client
    gc.collect()

    reopened = Client(str(database))
    assert len(reopened.execute("SELECT a FROM s.t;")) == 1
    reopened.close()


def test_connection_rollback_rolls_back_in_the_client_session(tmp_path):
    client, connection = _connection_with_a_table(tmp_path / "connection_rollback_db")
    assert connection.execute("BEGIN;").is_success()
    assert connection.execute("INSERT INTO s.t (a) VALUES (1);").is_success()
    rolled_back = connection.rollback()
    assert rolled_back.is_success()
    assert rolled_back.transaction_status == TransactionStatus.idle
    assert len(connection.execute("SELECT a FROM s.t;")) == 0
    connection.close()


def test_a_result_of_a_dropped_copy_is_refused_once_the_connection_closes(tmp_path):
    connection = _connection_with_an_open_insert(tmp_path / "dropped_copy_result_db")
    result = connection.cursor().execute("SELECT a FROM s.t;")
    gc.collect()
    connection.close()
    with pytest.raises(RuntimeError, match="the connection is closed"):
        result.fetchall()



_DEFAULT_CONNECTION_FROM_TWO_THREADS = """
import threading

import otterbrix

errors = []


def churn():
    for _ in range(30):
        for step in (lambda: otterbrix.connect("default").close(), lambda: otterbrix.connect("default")):
            try:
                step()
            except RuntimeError as error:
                if "another engine already owns" not in str(error):
                    errors.append(repr(error))


threads = [threading.Thread(target=churn) for _ in range(2)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
assert not errors, errors
print("DONE")
"""


# Only that nothing hangs: an open racing a close of the default connection may find the engine still there.
def test_two_threads_reopening_the_default_connection_never_hang(tmp_path):
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _DEFAULT_CONNECTION_FROM_TWO_THREADS],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd=str(tmp_path),
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("the threads reopening the default connection hung")
    _assert_finished(proc)
