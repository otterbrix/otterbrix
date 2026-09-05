"""Opening a connection must OPEN the database, never erase it.

`otterbrix.connect(path)` used to call std::filesystem::remove_all(path) inside
connection_environment_t::make_space, so pointing the binding at an existing
database silently destroyed it: the user's tables were gone before the first
statement ran. These tests are the gate on that.

Read-back goes through `Client`, not through the connection returned by
`connect()`: `OtterBrixPyConnection.execute` hands back the connection itself
and drops the cursor, so it cannot report rows. `Client` opens the very same
on-disk layout (both go through configuration::config::create_config) and does
return a countable cursor.
"""

import gc
import os
import shutil

import pytest

import otterbrix
from otterbrix import Client


def _clean_dir(name):
    path = os.path.join(os.getcwd(), name)
    if os.path.exists(path):
        shutil.rmtree(path)
    return path


def test_reopening_a_database_keeps_its_data():
    path = _clean_dir("test_connect_persistence_db")

    # 1. Create the database through the binding and put three rows in it.
    conn = otterbrix.connect(path)
    conn.execute("CREATE DATABASE persist;")
    conn.execute("CREATE TABLE persist.rows (id INTEGER, name TEXT);")
    conn.execute("INSERT INTO persist.rows (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');")
    conn.close()
    del conn
    gc.collect()

    # 2. Open the SAME path again. Nothing else happens here — the reopen alone
    #    is what used to wipe the directory.
    reopened = otterbrix.connect(path)
    reopened.close()
    del reopened
    gc.collect()

    # 3. The rows must still be there.
    client = Client(path)
    cursor = client.execute("SELECT * FROM persist.rows;")
    assert not cursor.is_error(), cursor.get_error()
    assert len(cursor) == 3
    cursor.close()


def test_connecting_to_a_foreign_directory_is_loud():
    """Rule 6: a directory that is not an otterbrix database is an error, not
    something to delete and replace with a fresh one."""
    path = _clean_dir("test_connect_persistence_foreign")
    os.makedirs(path)
    note = os.path.join(path, "notes.txt")
    with open(note, "w") as handle:
        handle.write("someone's documents")

    with pytest.raises(RuntimeError):
        otterbrix.connect(path)

    # And the refusal left the directory exactly as it found it.
    assert os.path.exists(note)
    with open(note) as handle:
        assert handle.read() == "someone's documents"


def test_unimplemented_connect_arguments_are_refused():
    """Rule 6: `read_only` and `config` are advertised by the `connect` binding
    (integration/python/main.cpp) and wired to nothing. Accepting them told the
    caller the request was honoured — read_only=True handed back a writable
    connection. They are refused until they mean something."""
    path = _clean_dir("test_connect_persistence_args")

    with pytest.raises(RuntimeError):
        otterbrix.connect(path, True)
    with pytest.raises(RuntimeError):
        otterbrix.connect(path, False, {"threads": 4})

    # Refused before anything was opened: no database was left behind.
    assert not os.path.exists(path)
