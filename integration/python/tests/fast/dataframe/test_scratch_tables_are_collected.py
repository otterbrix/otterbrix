"""Every chaining op on a relation materialises into a scratch table in the `tmp` database,
and those tables are PERSISTED with the database. Nothing ever removed them, so a database
directory that gets connected to over and over accumulated one `tmp.t<pid>_<n>` per operation,
for the life of the directory. There was no collector at all: no DROP TABLE and no DROP
DATABASE anywhere under integration/python.

The name is `t<pid>_<n>` (relation_factory.cpp), `n` counting from zero in each process, so the
one aggregate the first process below builds is `tmp.t<its pid>_0` and the second process can
ask about it by name. Asking is a CREATE: `collection already exists` is exactly how
make_aggregate_node's own retry loop detects a name that is taken, so it is the probe that is
already known to work on this catalog.

The same probe runs TWICE, and the first run is what keeps these cases from being vacuous.
"COLLECTED" on its own is also what a world where no scratch table was ever created looks
like -- rename the tables, or stop creating them, and a collector test that only probes
afterwards stays green while measuring nothing. So each builder first asks, WITH ITS SESSION
STILL OPEN, whether the name it expects is taken: it has to be ("already exists"), and only
then does the second process get to find it gone.
"""

import subprocess
import sys
import textwrap

_BUILD_ONE_AGGREGATE = """
import os
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
# limit() is a chaining op, so this is the one that allocates a scratch table.
assert len(rel.limit(2).fetchall()) == 2

# Positive control, taken while this session is still alive: the name has to be TAKEN now,
# or the collection this test checks for later is collection of nothing.
name = "tmp.t%d_0" % os.getpid()
try:
    conn.execute("CREATE TABLE %s();" % name)
except RuntimeError as e:
    assert "already exists" in str(e), e
    print("SCRATCH-EXISTS-DURING-SESSION")
else:
    raise AssertionError("%s was free while the relation that names it was alive" % name)
print("PID %d" % os.getpid())
"""

_PROBE = """
import sys

import otterbrix

conn = otterbrix.connect(sys.argv[1])
try:
    conn.execute("CREATE TABLE %s();" % sys.argv[2])
except RuntimeError as e:
    assert "already exists" in str(e), e
    print("STILL-THERE")
else:
    print("COLLECTED")
"""


def _run(script, *args):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *[str(a) for a in args]],
        capture_output=True,
        text=True,
    )


def _markers(finished):
    """The child's own prints. The engine logs to stdout too, and its trace is thousands of
    lines wide, so anything carrying a spdlog timestamp is dropped."""
    return [line for line in finished.stdout.splitlines() if not line.startswith("[")]


def test_a_session_collects_the_scratch_tables_it_created(tmp_path):
    database = tmp_path / "scratch_collection_db"

    built = _run(_BUILD_ONE_AGGREGATE, database)
    assert built.returncode == 0, built.stderr[-4000:]
    assert "SCRATCH-EXISTS-DURING-SESSION" in _markers(built), (
        "nothing was created to collect, so the probe below would prove nothing: %s"
        % (_markers(built),)
    )
    pid = next(line.split()[1] for line in _markers(built) if line.startswith("PID "))

    probed = _run(_PROBE, database, "tmp.t%s_0" % pid)
    assert probed.returncode == 0, probed.stderr[-4000:]
    assert "COLLECTED" in _markers(probed), (
        "the scratch table tmp.t%s_0 outlived the session that made it: %s"
        % (pid, _markers(probed))
    )


_BUILD_A_CHAIN = """
import os
import sys

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
# Two chaining ops, so two scratch tables: t<pid>_0 and t<pid>_1.
assert len(rel.limit(3).limit(2).fetchall()) == 2

# Positive control for BOTH of them, while the session that made them is still open.
for index in (0, 1):
    name = "tmp.t%d_%d" % (os.getpid(), index)
    try:
        conn.execute("CREATE TABLE %s();" % name)
    except RuntimeError as e:
        assert "already exists" in str(e), e
    else:
        raise AssertionError("%s was free while the relation that names it was alive" % name)
print("SCRATCH-EXISTS-DURING-SESSION")
print("PID %d" % os.getpid())
"""


def test_every_scratch_table_of_a_chain_is_collected_not_just_the_last(tmp_path):
    database = tmp_path / "scratch_chain_db"

    built = _run(_BUILD_A_CHAIN, database)
    assert built.returncode == 0, built.stderr[-4000:]
    assert "SCRATCH-EXISTS-DURING-SESSION" in _markers(built), (
        "nothing was created to collect, so the probes below would prove nothing: %s"
        % (_markers(built),)
    )
    pid = next(line.split()[1] for line in _markers(built) if line.startswith("PID "))

    for index in (0, 1):
        name = "tmp.t%s_%d" % (pid, index)
        probed = _run(_PROBE, database, name)
        assert probed.returncode == 0, probed.stderr[-4000:]
        assert "COLLECTED" in _markers(probed), (
            "%s outlived the session that made it: %s" % (name, _markers(probed))
        )
