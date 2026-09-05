"""A relation materialises into a scratch table in the `tmp` database, and that table is
PERSISTED with the database while the counter that names it lives only as long as the process.

So the failing case is not exotic: open the same database from a second process and the very
first relation asks for a name the first process left behind. Before the fix, running any of
the sibling dataframe test files twice against one database directory turned "4 passed" into
"4 failed", each of them

    RuntimeError: relation: creating the scratch table tmp.t2 failed: collection already exists

This test carries that shape in one file: the same database directory, three separate
processes, each of which must be able to build a relation.
"""

import subprocess
import sys
import textwrap

_BUILD_A_RELATION = textwrap.dedent(
    """
    import sys

    import pandas as pd

    import otterbrix

    conn = otterbrix.connect(sys.argv[1])
    rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
    rows = rel.limit(2).fetchall()
    assert len(rows) == 2, rows
    conn.close()
    print("RELATION-OK")
    """
)


def test_every_process_opening_one_database_can_build_a_relation(tmp_path):
    database = str(tmp_path / "relation_scratch_db")

    for run in (1, 2, 3):
        finished = subprocess.run(
            [sys.executable, "-c", _BUILD_A_RELATION, database],
            capture_output=True,
            text=True,
        )
        # The MARKER, not the exit status: the module currently dies with SIGSEGV during
        # interpreter finalisation whenever a relation was built at all (reproducible with
        # connect + from_df + fetchall and nothing else), which would mask this result.
        # That teardown crash is a separate defect and is not what this test measures.
        assert "RELATION-OK" in finished.stdout, (
            "run %d against the same database did not build its relation:\n%s"
            % (run, finished.stderr[-4000:])
        )
