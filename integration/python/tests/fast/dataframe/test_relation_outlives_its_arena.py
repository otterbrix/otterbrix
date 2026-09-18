"""A relation's plan is pmr-allocated out of the ENGINE's arena, so the relation cannot be
allowed to outlive the space that owns that arena.

`py_result_t` already knows this and holds `boost::intrusive_ptr<otterbrix_t> space` for
exactly this reason (integration/python/otterbrix_wrapper/pyresult.hpp). `py_relation_t` held
`node_` and `schema_` out of the same arena and kept NO reference to the space, so freeing them
reached a memory_resource that was already gone:

    EXC_BAD_ACCESS (code=1, address=0x18)
      std::pmr::memory_resource::deallocate(this=0xaf60d8298, __p=..., __bytes=152, __align=8)
      std::pmr::vector<data_chunk_t>::~vector
      components::logical_plan::node_data_t::~node_data_t
      boost::intrusive_ptr<node_t>::reset
      otterbrix::py_relation_t::~py_relation_t   at pyrelation.cpp:68
      pybind11::class_<otterbrix::py_relation_t>::dealloc
      Python`finalize_modules / _Py_Finalize

The exit status is the whole point of these cases, so each one runs in its own interpreter:
a crash during `Py_Finalize` happens after the last statement has already run and printed, so
nothing inside the process can observe it.
"""

import subprocess
import sys
import textwrap

# The relation stays reachable from a module global, so the interpreter is still holding it
# when it tears the modules down. Nothing is closed and nothing is deleted: this is the plain
# shape of every dataframe session that ends by falling off the end of the script.
_ALIVE_AT_EXIT = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
assert len(rel.limit(2).fetchall()) == 2
print("ALIVE-AT-EXIT-OK")
"""

# close() drops what can be the last reference to the space -- the arena the relation's plan
# lives in -- and the relation is only released afterwards.
_CLOSED_BEFORE_RELEASE = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
assert len(rel.limit(2).fetchall()) == 2
conn.close()
del conn
print("CLOSED-BEFORE-RELEASE-OK", flush=True)
del rel
print("RELEASED-OK")
"""


# The relation reaches back into the connection for every chaining op, so dropping the
# connection object while a relation is still in hand must not leave that back-pointer
# hanging. Debug builds caught the freed read as `Assertion failed: (px != 0)` inside
# intrusive_ptr::operator->; with NDEBUG the assert is gone and the read stands.
_CONNECTION_DROPPED_FIRST = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
del conn
print("CONNECTION-DROPPED", flush=True)
assert len(rel.limit(2).fetchall()) == 2
print("STILL-USABLE-OK")
"""

# A closed connection has no space, and executing a relation on it went straight to
# `space->dispatcher()`. `execute_internal` already refuses this case out loud
# ("the connection is closed"); the relation road had no such check, so it aborted on the
# null intrusive_ptr in Debug and dereferenced it under NDEBUG.
_EXECUTED_AFTER_CLOSE = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
conn.close()
try:
    rel.fetchall()
except RuntimeError as e:
    assert "closed" in str(e), e
    print("REFUSED-OK")
else:
    raise AssertionError("executing on a closed connection was not refused")
"""


# BUILDING a relation on a closed connection is the same defect one step earlier, and the
# guard added above does not reach it. `from_df` allocates its node and schema out of
# `relation_factory_t::space` -- the copy close() deliberately LEAVES ALIVE so the scratch
# tables can still be dropped -- while `py_relation_t::space_` is taken from the copy close()
# NULLS. The relation therefore came out holding memory from a live arena and no reference to
# it, and freeing it at interpreter shutdown reached a resource that was already gone. Same
# stack as the header, one allocation later:
#
#     EXC_BAD_ACCESS (code=1, address=0x18)
#       std::pmr::memory_resource::deallocate(__bytes=384)
#       std::pmr::vector<column_definition_t>::~vector      <- schema_
#       otterbrix::py_relation_t::~py_relation_t
#
# Nothing asserts on this road, so Debug and NDEBUG behave alike: a silent use-after-free.
_BUILT_AFTER_CLOSE = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
conn.close()
try:
    conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
except RuntimeError as e:
    assert "closed" in str(e), e
    print("BUILD-REFUSED-OK")
else:
    raise AssertionError("building a relation on a closed connection was not refused")
"""

# The same road reached through an EXISTING relation. Chaining does not go near
# `py_connection_t::space` at all -- `limit_relation` allocates from the factory copy and even
# creates its scratch table -- so a closed connection kept handing out new relations with a
# null `space_`.
_CHAINED_AFTER_CLOSE = """
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
assert len(rel.limit(2).fetchall()) == 2
conn.close()
try:
    rel.limit(1)
except RuntimeError as e:
    assert "closed" in str(e), e
    print("CHAIN-REFUSED-OK")
else:
    raise AssertionError("chaining on a closed connection was not refused")
"""


def _run(script, database):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), str(database)],
        capture_output=True,
        text=True,
    )


def _assert_clean_exit(finished, marker):
    assert marker in finished.stdout, "the case did not get as far as its marker:\n%s" % (
        finished.stderr[-4000:],
    )
    # Negative returncode is the signal that killed it: -11 is SIGSEGV.
    assert finished.returncode == 0, (
        "the interpreter did not exit cleanly: returncode=%d\n%s"
        % (finished.returncode, finished.stderr[-4000:])
    )


def test_a_relation_still_held_at_interpreter_shutdown_does_not_crash(tmp_path):
    finished = _run(_ALIVE_AT_EXIT, tmp_path / "alive_at_exit_db")
    _assert_clean_exit(finished, "ALIVE-AT-EXIT-OK")


def test_a_relation_released_after_its_connection_was_closed_does_not_crash(tmp_path):
    finished = _run(_CLOSED_BEFORE_RELEASE, tmp_path / "closed_before_release_db")
    _assert_clean_exit(finished, "RELEASED-OK")


def test_a_relation_still_works_after_its_connection_object_is_dropped(tmp_path):
    finished = _run(_CONNECTION_DROPPED_FIRST, tmp_path / "connection_dropped_db")
    _assert_clean_exit(finished, "STILL-USABLE-OK")


def test_executing_a_relation_on_a_closed_connection_is_refused_not_fatal(tmp_path):
    finished = _run(_EXECUTED_AFTER_CLOSE, tmp_path / "executed_after_close_db")
    _assert_clean_exit(finished, "REFUSED-OK")


def test_building_a_relation_on_a_closed_connection_is_refused_not_fatal(tmp_path):
    finished = _run(_BUILT_AFTER_CLOSE, tmp_path / "built_after_close_db")
    _assert_clean_exit(finished, "BUILD-REFUSED-OK")


def test_chaining_a_relation_on_a_closed_connection_is_refused_not_fatal(tmp_path):
    finished = _run(_CHAINED_AFTER_CLOSE, tmp_path / "chained_after_close_db")
    _assert_clean_exit(finished, "CHAIN-REFUSED-OK")
