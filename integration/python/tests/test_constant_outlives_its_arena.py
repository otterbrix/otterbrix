"""A constant's bytes are pmr-allocated out of the ENGINE's arena, so the map that keeps
them must be destroyed BEFORE the space that owns that arena.

`ConstantExpression("OH", conn)` builds a `logical_value_t` on
`conn.space_ptr()->dispatcher()->resource()` and hands it to
`expression_factory_t::add_value`, which stores it in the member map `values`. That member map
and the `boost::intrusive_ptr<otterbrix_t> space` that owns the arena live side by side in
expression_factory_t, and members are destroyed in REVERSE declaration order: `space` was
declared last, so the arena went first and the map freed its strings into a memory_resource
that no longer existed.

    EXC_BAD_ACCESS (code=1, address=0x18)
      std::pmr::memory_resource::deallocate(this=0x100c55f48, __p=0x5b61484c0, __bytes=24, __align=8)
      ... ~logical_value_t
      otterbrix::expression_factory_t::~expression_factory_t
      Python`finalize_modules / _Py_Finalize

There is a second half, reached by close(): close() nulls expression_factory_t's own space
and deliberately leaves relation_factory_t's alive, so after a close the last reference to
the arena is relation_factory_t's -- and py_connection_t listed expression_factory_t as its
FIRST base, which under reverse-order base destruction made it the LAST to die. Same crash,
one level up. Both halves are fixed by declaring the space-holder first.

Only a constant whose payload actually allocates is affected: `ConstantExpression(7, conn)`
stores an integer inline and exits cleanly, `ConstantExpression("OH", conn)` stored a string
and died 10 runs out of 10. (That is also why the existing expression-lifetime cases next
door never caught it: their constants are integers.) Nothing in the process can see it -- the
crash lands after the last statement has printed -- so, like test_relation_outlives_its_arena.py,
each case runs in its own interpreter and the EXIT STATUS is the assertion.
"""

import subprocess
import sys
import textwrap

# The plain shape: build a string constant and let the interpreter tear the module down with
# both the connection and the expression still reachable. Nothing is closed, nothing deleted.
_STRING_CONSTANT_ALIVE_AT_EXIT = """
import sys

import otterbrix
from otterbrix import ConstantExpression

conn = otterbrix.connect(sys.argv[1])
constant = ConstantExpression("OH", conn)
print("STRING-CONSTANT-OK")
"""

# Dropping the Python object first does NOT help, and that is the point: the bytes are not in
# the expression object, they are in the factory's `values` map, which is only destroyed with
# the connection. A fix that merely made the Python wrapper hold a reference would pass the
# case above and still fail this one.
_STRING_CONSTANT_RELEASED_EARLY = """
import gc
import sys

import otterbrix
from otterbrix import ConstantExpression

conn = otterbrix.connect(sys.argv[1])
constant = ConstantExpression("OH", conn)
del constant
gc.collect()
conn.close()
print("RELEASED-EARLY-OK")
"""

# The comparison the dataframe tests actually build, so the case is the user-visible one and
# not just the constructor: a column, a string constant and an `==` between them.
_STRING_CONSTANT_IN_A_COMPARISON = """
import sys

import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

conn = otterbrix.connect(sys.argv[1])
condition = ColumnExpression("state", conn) == ConstantExpression("OH", conn)
# The constant prints as its parameter slot (#0), not as its text: the text is exactly what
# lives in the factory's `values` map, which is the memory this case is about.
assert str(condition) == '"state": {$eq: #0}', condition
print("COMPARISON-OK")
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


def test_a_string_constant_held_at_interpreter_shutdown_does_not_crash(tmp_path):
    finished = _run(_STRING_CONSTANT_ALIVE_AT_EXIT, tmp_path / "string_constant_db")
    _assert_clean_exit(finished, "STRING-CONSTANT-OK")


def test_a_string_constant_released_before_its_connection_does_not_crash(tmp_path):
    finished = _run(_STRING_CONSTANT_RELEASED_EARLY, tmp_path / "released_early_db")
    _assert_clean_exit(finished, "RELEASED-EARLY-OK")


def test_a_string_constant_inside_a_comparison_does_not_crash(tmp_path):
    finished = _run(_STRING_CONSTANT_IN_A_COMPARISON, tmp_path / "comparison_db")
    _assert_clean_exit(finished, "COMPARISON-OK")
