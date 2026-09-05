"""An `Expression` must not die with the connection that produced it.

`py_expression_t` held its `expression_wrapper_t` BY VALUE and the connection
only as a raw `expression_factory_t*`. Two different things it reads back later
belong to the connection:

  * a COLUMN expression is a `key_t` whose characters are `std::pmr::string`s cut
    from the space's arena, and that arena is a MEMBER of the space
    (`base_otterbrix_t::resource`, integration/cpp/base_spaces.hpp), so it dies
    with the space;
  * a CONSTANT expression is a parameter id, and the value it names lives in
    `expression_factory_t::values` -- a member of the connection OBJECT.

The expression held neither. So the moment Python released the connection --
which it does routinely, because the locals of a finished frame go in no
guaranteed order, and because `ColumnExpression("x", connect(path))` never binds
the connection at all -- reading the expression read freed memory, and
`~py_expression_t` handed the key's bytes back to a pool that no longer existed.

`close()` alone does NOT free the arena: `relation_factory_t::space` is a second
reference that `close()` deliberately leaves alive (see relation_factory.cpp).
It is the connection OBJECT going away that frees it. Both are covered below,
and the closed case is here for a second reason: reading an expression back
touches the key / the value / the built node, never the space, so a closed
connection must still be able to print the expressions it made.

The third case is the same lifetime seen from the other side. Every road that
BUILDS an expression dereferences `expression_factory_t::space`, the copy
`close()` nulls, and `boost::intrusive_ptr::operator->` on a null pointer is an
assert that NDEBUG deletes -- SIGABRT in a debug build, a null dereference in
the build that ships. That has to be a refusal Python can catch.

A crash cannot be asserted from inside the process it kills, so each case runs
in a child and the parent reads its exit code.
"""

import os
import shutil
import subprocess
import sys

# 64 columns whose names are far past std::pmr::string's small-string buffer, so
# the characters really do live in the arena instead of inside the key object,
# and 64 constants, whose values live in the connection object instead.
_PREAMBLE = """
import gc, sys
from otterbrix import connect, ColumnExpression, ConstantExpression, CountExpression

path = sys.argv[1]
N = 64
NAMES = ["c%03d_%s" % (i, "a" * 72) for i in range(N)]
VALUES = [700000 + i for i in range(N)]

def read(e):
    try:
        return repr(e)
    except Exception as exc:
        return "EXC:" + type(exc).__name__

conn = connect(path)
exprs = [ColumnExpression(n, conn) for n in NAMES]
consts = [ConstantExpression(v, conn) for v in VALUES]
WANT = NAMES + [str(v) for v in VALUES]

def check():
    got = [read(e) for e in exprs + consts]
    bad = [i for i, (g, w) in enumerate(zip(got, WANT)) if g != w]
    print("MISMATCHES=%d/%d" % (len(bad), len(WANT)))
    if bad:
        print("FIRST_BAD=%d %r" % (bad[0], got[bad[0]][:96]))

assert [read(e) for e in exprs + consts] == WANT, "the expressions were already wrong BEFORE the release"
print("PRE_OK")
"""

# The connection object itself goes away. `__del__` is bound to close(), so this
# is close() AND the last reference dropping.
_RELEASE_CONNECTION = """
del conn
gc.collect()
check()
del exprs, consts
gc.collect()
"""

# close() only: the connection object stays, and the expressions stay readable.
# Nothing is deleted by hand -- interpreter shutdown releases everything in
# whatever order it likes, which is exactly the order that used to crash.
_CLOSE_CONNECTION = """
conn.close()
check()
"""

# Building on a closed connection must be a refusal, not an abort.
_BUILD_AFTER_CLOSE = """
conn.close()
refused = 0
for name, call in (
    ("CountExpression", lambda: CountExpression(conn)),
    ("sum", lambda: exprs[0].sum()),
    ("negate", lambda: -exprs[0]),
    ("alias", lambda: exprs[0].alias("a")),
    ("asc", lambda: exprs[0].asc()),
    ("add", lambda: exprs[0] + exprs[1]),
):
    try:
        call()
        print("NOT_REFUSED=%s" % name)
    except RuntimeError:
        refused += 1
print("REFUSED=%d/6" % refused)
check()
"""


def _run(body, dirname):
    path = os.path.join(os.getcwd(), dirname)
    if os.path.exists(path):
        shutil.rmtree(path)
    return subprocess.run(
        [sys.executable, "-c", _PREAMBLE + body + '\nprint("SURVIVED")\n', path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


def _assert_intact(proc):
    assert proc.returncode == 0, "process died with %s\n%s" % (proc.returncode, proc.stderr[-2000:])
    assert "PRE_OK" in proc.stdout, proc.stdout
    assert "MISMATCHES=0/128" in proc.stdout, proc.stdout
    assert "SURVIVED" in proc.stdout, proc.stdout


def test_an_expression_survives_its_connection_being_released():
    proc = _run(_RELEASE_CONNECTION, "test_expression_lifetime_released")
    _assert_intact(proc)


def test_an_expression_stays_readable_after_its_connection_is_closed():
    proc = _run(_CLOSE_CONNECTION, "test_expression_lifetime_closed")
    _assert_intact(proc)


def test_building_on_a_closed_connection_is_refused_not_aborted():
    proc = _run(_BUILD_AFTER_CLOSE, "test_expression_lifetime_build_after_close")
    _assert_intact(proc)
    assert "REFUSED=6/6" in proc.stdout, proc.stdout
