"""A `Cursor` must not die with the `Client` that produced it.

`wrapper_cursor` holds the result batch (pmr-allocated from the space's memory
resource) and a raw `wrapper_dispatcher_t*`, but held no reference to the space
itself. `wrapper_client` owned the only one. So dropping the client first --
which Python does routinely, because locals of a finished frame are released in
no guaranteed order -- freed the arena the cursor's rows lived in and the next
touch of the cursor took the process down with SIGSEGV.

That is what killed `pytest` mid-run: `test_connect_persistence.py` ends a test
holding both a `Client` and a `Cursor`, so the whole session died there and no
test after it ran at all.

The crash cannot be asserted from inside the process it kills, so each case runs
in a child and the parent reads its exit code.
"""

import os
import shutil
import subprocess
import sys

_BODY = """
import gc, os, sys
from otterbrix import Client

path = sys.argv[1]
client = Client(path)
client.execute("CREATE DATABASE d;")
client.execute("CREATE TABLE d.t (id INTEGER);")
client.execute("INSERT INTO d.t (id) VALUES (1), (2);")
cursor = client.execute("SELECT id FROM d.t;")
assert len(cursor) == 2

%s

print("SURVIVED")
"""

_DROP_CLIENT_FIRST = """
del client
gc.collect()
assert len(cursor) == 2
del cursor
gc.collect()
"""

_DROP_CURSOR_FIRST = """
del cursor
gc.collect()
del client
gc.collect()
"""


def _run(release_order, dirname):
    path = os.path.join(os.getcwd(), dirname)
    if os.path.exists(path):
        shutil.rmtree(path)
    return subprocess.run(
        [sys.executable, "-c", _BODY % release_order, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


def test_a_cursor_survives_its_client_being_released_first():
    proc = _run(_DROP_CLIENT_FIRST, "test_cursor_lifetime_client_first")
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "SURVIVED" in proc.stdout, proc.stdout


def test_a_cursor_released_before_its_client_still_works():
    """The order that already worked -- kept so the fix cannot regress it."""
    proc = _run(_DROP_CURSOR_FIRST, "test_cursor_lifetime_cursor_first")
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "SURVIVED" in proc.stdout, proc.stdout
