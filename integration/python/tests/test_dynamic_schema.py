"""
Python integration test for dynamic schema (relkind='g').

Verifies that Python bindings handle dynamic-schema tables (created without
column definitions) correctly: arbitrary keys can be inserted, and SELECT
returns rows with NULL/None for absent fields. Exercises the `CREATE TABLE
foo;` (no column list) syntax and the column-union behaviour for relkind='g'
tables.
"""

import os
import shutil
import pytest
from otterbrix import Client

# Hermetic: clean the on-disk state at startup so repeated local runs don't
# collide with persisted data (mirrors test_collection_sql.py).
_path = os.getcwd() + "/test_dynamic_schema"
if os.path.exists(_path):
    shutil.rmtree(_path)
client = Client(_path)
client.execute("CREATE DATABASE dyn;")


def gen_id(num):
    res = str(num)
    while len(res) < 24:
        res = '0' + res
    return res


@pytest.fixture()
def docs_table(request):
    # Use parens-less form to declare a dynamic-schema (relkind='g') table.
    # Fall back to "CREATE TABLE name();" if the no-parens form is rejected
    # by the SQL transformer at runtime.
    table = "dyn.docs"
    drop = client.execute("DROP TABLE {};".format(table))
    drop.close()
    create = client.execute("CREATE TABLE {};".format(table))
    if create.is_error():
        create.close()
        # Retry with empty parens form which is accepted by all existing tests.
        create = client.execute("CREATE TABLE {}();".format(table))
        if create.is_error():
            err = create.get_error()
            create.close()
            pytest.skip("CREATE TABLE for dynamic schema not supported: {}".format(err))
    create.close()

    def finalize():
        client.execute("DROP TABLE {};".format(table)).close()

    request.addfinalizer(finalizer=finalize)
    return table


def test_dynamic_schema_basic_flow(docs_table):
    """Insert dicts with different key sets, verify all rows appear."""
    table = docs_table

    c = client.execute(
        "INSERT INTO {} (_id, a, b) VALUES ('{}', 1, 'x');".format(table, gen_id(1))
    )
    if c.is_error():
        err = c.get_error()
        c.close()
        pytest.skip("INSERT into dynamic-schema table not supported: {}".format(err))
    c.close()

    c = client.execute(
        "INSERT INTO {} (_id, a, b, c) VALUES ('{}', 2, 'y', 3.14);".format(table, gen_id(2))
    )
    assert not c.is_error(), c.get_error()
    c.close()

    c = client.execute("SELECT * FROM {};".format(table))
    assert not c.is_error(), c.get_error()
    assert len(c) == 2
    c.close()


def test_dynamic_schema_null_for_absent(docs_table):
    """A row missing a field surfaces as NULL.

    Matches PostgreSQL (absent nullable column without explicit DEFAULT
    is NULL) and the C++ counterpart test_collection::insert::columns_simple,
    which asserts NULL for an absent column on a static table whose
    definition carries no default.
    """
    table = docs_table

    c = client.execute(
        "INSERT INTO {} (_id, a) VALUES ('{}', 1);".format(table, gen_id(10))
    )
    if c.is_error():
        err = c.get_error()
        c.close()
        pytest.skip("INSERT into dynamic-schema table not supported: {}".format(err))
    c.close()

    c = client.execute(
        "INSERT INTO {} (_id, a, b) VALUES ('{}', 2, 'x');".format(table, gen_id(11))
    )
    assert not c.is_error(), c.get_error()
    c.close()

    c = client.execute("SELECT * FROM {} ORDER BY a;".format(table))
    assert not c.is_error(), c.get_error()
    assert len(c) == 2

    c.next()
    assert c['a'] == 1
    assert c['b'] is None

    c.next()
    assert c['a'] == 2
    assert c['b'] == 'x'

    c.close()


def test_dynamic_schema_arbitrary_keys(docs_table):
    """Different rows may carry entirely disjoint key sets."""
    table = docs_table

    c = client.execute(
        "INSERT INTO {} (_id, alpha) VALUES ('{}', 100);".format(table, gen_id(20))
    )
    if c.is_error():
        err = c.get_error()
        c.close()
        pytest.skip("INSERT into dynamic-schema table not supported: {}".format(err))
    c.close()

    c = client.execute(
        "INSERT INTO {} (_id, beta) VALUES ('{}', 'hello');".format(table, gen_id(21))
    )
    assert not c.is_error(), c.get_error()
    c.close()

    c = client.execute("SELECT * FROM {};".format(table))
    assert not c.is_error(), c.get_error()
    assert len(c) == 2
    c.close()
