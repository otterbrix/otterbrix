"""`connect(...).execute(...)` must hand back the rows, and must say so when it fails.

`OtterBrixPyConnection.execute` ran the statement through `execute_internal`,
threw the resulting cursor away and returned `shared_from_this()`. On the most
visible surface of the product that meant:

  * a SELECT produced no rows any caller could reach -- `execute` returned the
    connection object, which has no fetch method at all;
  * a statement the engine rejected was indistinguishable from one it accepted:
    the same connection came back either way and the cursor carrying
    `core::error_t` went on the floor;
  * a query that was not a `str` was never executed -- the isinstance check fell
    through to the same `return`, so the call was a silent no-op;
  * an empty query walked into `linitial()` on an empty parse list and took the
    process down (SIGSEGV) at exit;
  * a statement on a closed connection dereferenced the null space and aborted
    the process (SIGABRT).

These tests assert on CONTENT -- the rows themselves and the engine's own error
text -- not merely on the type that comes back.
"""

import os
import shutil
import subprocess
import sys

import pytest

import otterbrix


def _clean_dir(name):
    path = os.path.join(os.getcwd(), name)
    if os.path.exists(path):
        shutil.rmtree(path)
    return path


@pytest.fixture(scope="module")
def conn():
    connection = otterbrix.connect(_clean_dir("test_connection_execute_db"))
    connection.execute("CREATE DATABASE ex;")
    connection.execute("CREATE TABLE ex.rows (id INTEGER, name TEXT);")
    connection.execute("INSERT INTO ex.rows (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');")
    yield connection
    connection.close()


def test_select_hands_back_the_rows_it_selected(conn):
    result = conn.execute("SELECT id, name FROM ex.rows ORDER BY id;")
    assert result.fetchall() == [(1, "a"), (2, "b"), (3, "c")]


def test_select_rows_are_reachable_one_at_a_time(conn):
    result = conn.execute("SELECT id FROM ex.rows ORDER BY id;")
    assert result.fetchone() == (1,)
    assert result.fetchmany(2) == [(2,), (3,)]
    assert result.fetchone() is None


def test_a_write_reports_how_many_rows_it_wrote(conn):
    conn.execute("CREATE TABLE ex.writes (id INTEGER);")
    written = conn.execute("INSERT INTO ex.writes (id) VALUES (7), (8);")
    assert len(written) == 2
    assert conn.execute("SELECT id FROM ex.writes ORDER BY id;").fetchall() == [(7,), (8,)]


def test_select_rows_are_reachable_as_a_dataframe(conn):
    pd = pytest.importorskip("pandas")
    frame = conn.execute("SELECT id, name FROM ex.rows ORDER BY id;").df()
    assert isinstance(frame, pd.DataFrame)
    assert list(frame["id"]) == [1, 2, 3]
    assert list(frame["name"]) == ["a", "b", "c"]


def test_a_statement_the_engine_rejects_raises_with_the_engine_message(conn):
    with pytest.raises(RuntimeError) as excinfo:
        conn.execute("SELECT * FROM nosuchdb.nosuchtable;")
    assert "nosuchdb" in str(excinfo.value)


def test_a_statement_the_parser_rejects_raises_with_the_engine_message(conn):
    with pytest.raises(RuntimeError) as excinfo:
        conn.execute("SELCT 1;")
    assert "SELCT" in str(excinfo.value)


def test_execute_refuses_a_query_that_is_not_a_string(conn):
    with pytest.raises(TypeError):
        conn.execute(42)
    with pytest.raises(TypeError):
        conn.execute(None)
    with pytest.raises(TypeError):
        conn.execute(["SELECT 1;"])


def test_a_result_outlives_the_connection_that_produced_it():
    """The rows must still be readable after the connection is closed: the
    result owns the batch it was handed, not a borrowed view into a space that
    `close()` may have been the last reference to."""
    connection = otterbrix.connect(_clean_dir("test_connection_execute_outlive"))
    connection.execute("CREATE DATABASE ol;")
    connection.execute("CREATE TABLE ol.rows (id INTEGER);")
    connection.execute("INSERT INTO ol.rows (id) VALUES (11), (12);")
    result = connection.execute("SELECT id FROM ol.rows ORDER BY id;")
    connection.close()
    del connection
    assert result.fetchall() == [(11,), (12,)]


def _run_probe(body, dirname):
    """A crash cannot be asserted from inside the process it kills, so the two
    statements below run in a child and the parent inspects its exit code."""
    script = "import sys\nimport otterbrix\nconn = otterbrix.connect(sys.argv[1])\n" + body
    proc = subprocess.run(
        [sys.executable, "-c", script, _clean_dir(dirname)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return proc


_REFUSE = (
    "try:\n"
    "    conn.execute(%s)\n"
    "except RuntimeError as exc:\n"
    '    print("REFUSED:%%s" %% exc)\n'
    "else:\n"
    '    print("ACCEPTED")\n'
)


def test_an_empty_query_is_refused_instead_of_taking_the_process_down():
    """`execute_internal` called `linitial()` on the parse list without checking
    it, so an empty statement dereferenced nothing and the process died with
    SIGSEGV -- after `execute` had already told the caller all was well."""
    proc = _run_probe(_REFUSE % '""' + "conn.close()\n", "test_connection_execute_empty")
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "REFUSED:" in proc.stdout, proc.stdout


def test_a_statement_on_a_closed_connection_is_refused_instead_of_aborting():
    """`close()` drops the space; `execute_internal` then called
    `space->dispatcher()` on a null intrusive_ptr and the process aborted."""
    proc = _run_probe("conn.close()\n" + _REFUSE % '"SELECT 1;"', "test_connection_execute_closed")
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "REFUSED:" in proc.stdout, proc.stdout
