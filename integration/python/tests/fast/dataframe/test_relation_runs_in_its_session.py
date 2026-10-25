"""A relation is a plan run in its connection's session; building one writes nothing to the catalog."""

import subprocess
import sys
import textwrap

import pandas as pd
import pytest

import otterbrix
from otterbrix import TransactionStatus


def _frame():
    return pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]})


def _relnames(conn):
    return sorted(row[0] for row in conn.execute("SELECT relname FROM pg_catalog.pg_class;").fetchall())


def _nspnames(conn):
    return sorted(row[0] for row in conn.execute("SELECT nspname FROM pg_catalog.pg_namespace;").fetchall())


def _id_above(conn, threshold):
    return otterbrix.ColumnExpression("id", conn) > otterbrix.ConstantExpression(threshold, conn)


# In a child so that its pid and its first relation are known: the name a scratch table would
# take, tmp.t<pid>_0, is created before the relation is built.
_TAKEN_NAME = """
import os
import sys

import pandas as pd

import otterbrix
from otterbrix import TransactionStatus

conn = otterbrix.connect(sys.argv[1])
try:
    conn.execute("CREATE DATABASE tmp;")
except RuntimeError as e:
    assert "already exists" in str(e), e
conn.execute("CREATE TABLE tmp.t%d_0 ();" % os.getpid())
rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}))
assert conn.execute("BEGIN;").transaction_status == TransactionStatus.in_transaction
rows = rel.limit(2).fetchall()
assert len(rows) == 2, rows
assert conn.execute("SELECT 1;").transaction_status == TransactionStatus.in_transaction
print("ROWS-IN-TRANSACTION-OK")
"""


def _markers(finished):
    return [line for line in finished.stdout.splitlines() if not line.startswith("[")]


def test_a_relation_inside_begin_does_not_collide_with_an_existing_table(tmp_path):
    finished = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_TAKEN_NAME), str(tmp_path / "taken_name_db")],
        capture_output=True,
        text=True,
    )
    assert "ROWS-IN-TRANSACTION-OK" in _markers(finished), finished.stderr[-2000:]


def test_a_rolled_back_relation_leaves_no_table(tmp_path):
    conn = otterbrix.connect(str(tmp_path / "rolled_back_db"))
    try:
        tables = _relnames(conn)
        rel = conn.from_df(_frame())
        conn.execute("BEGIN;")
        assert sorted(rel.filter(_id_above(conn, 2)).fetchall()) == [(3, 30), (4, 40)]
        assert conn.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
        assert _relnames(conn) == tables
        assert "tmp" not in _nspnames(conn)
    finally:
        conn.close()


def test_a_relation_in_a_failed_block_fails_when_run_not_when_built(tmp_path):
    conn = otterbrix.connect(str(tmp_path / "failed_block_db"))
    try:
        rel = conn.from_df(_frame())
        conn.execute("BEGIN;")
        with pytest.raises(RuntimeError):
            conn.execute("SELECT no_such_column FROM no_such_db.no_such_table;")
        filtered = rel.filter(_id_above(conn, 2))
        with pytest.raises(RuntimeError) as refused:
            filtered.fetchall()
        assert refused.value.transaction_status == TransactionStatus.failed
        assert conn.execute("ROLLBACK;").transaction_status == TransactionStatus.idle
    finally:
        conn.close()


def test_relation_chains_add_nothing_to_the_catalog(tmp_path):
    conn = otterbrix.connect(str(tmp_path / "no_catalog_rows_db"))
    try:
        tables = _relnames(conn)
        rel = conn.from_df(_frame())
        chained = rel.filter(_id_above(conn, 1)).limit(2)
        assert len(chained.fetchall()) == 2
        assert _relnames(conn) == tables
    finally:
        conn.close()
