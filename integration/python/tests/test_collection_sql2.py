import os
import pytest
from otterbrix import Client

database_name = "testdb2"
collection_name = "testcol2"

client = Client(os.getcwd() + "/test_collection_sql2")
client.execute("CREATE DATABASE {};".format(database_name))
client.execute("CREATE TABLE {}.{}();".format(database_name, collection_name))


def gen_id(num):
    res = str(num)
    while len(res) < 24:
        res = '0' + res
    return res


@pytest.fixture()
def col(request):
    client.execute("CREATE DATABASE {};".format(database_name))
    client.execute("CREATE TABLE {}.{}();".format(database_name, collection_name))

    # Insert 100 rows with various types
    query = "INSERT INTO {}.{} (_id, count, count_str, countFloat, count_bool) VALUES ".format(
        database_name, collection_name)
    rows = []
    for num in range(100):
        _id = gen_id(num)
        count_str = str(num)
        count_float = num + 0.1
        count_bool = 'TRUE' if num % 2 != 0 else 'FALSE'
        rows.append("('{}', {}, '{}', {}, {})".format(_id, num, count_str, count_float, count_bool))
    query += ", ".join(rows) + ";"
    c = client.execute(query)
    assert len(c) == 100
    c.close()

    def finalize():
        client.execute("DROP TABLE {}.{};".format(database_name, collection_name)).close()

    request.addfinalizer(finalizer=finalize)
    return client


def test_select_all(col):
    c = col.execute("SELECT * FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 100
    c.close()


def test_select_count(col):
    c = col.execute("SELECT COUNT(*) FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 1
    c.close()


def test_select_where_eq(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count = 50;".format(database_name, collection_name))
    assert len(c) == 1
    c.close()


def test_select_where_gt(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count > 90;".format(database_name, collection_name))
    assert len(c) == 9
    c.close()


def test_select_where_gte(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count >= 90;".format(database_name, collection_name))
    assert len(c) == 10
    c.close()


def test_select_where_lt(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count < 10;".format(database_name, collection_name))
    assert len(c) == 10
    c.close()


def test_select_where_lte(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count <= 10;".format(database_name, collection_name))
    assert len(c) == 11
    c.close()


def test_select_where_ne(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count != 50;".format(database_name, collection_name))
    assert len(c) == 99
    c.close()


def test_select_where_and(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count > 10 AND count <= 50;".format(
        database_name, collection_name))
    assert len(c) == 40
    c.close()


def test_select_where_or(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count < 10 OR count >= 90;".format(
        database_name, collection_name))
    assert len(c) == 20
    c.close()


def test_select_where_bool(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count_bool = TRUE;".format(
        database_name, collection_name))
    assert len(c) == 50
    c.close()


def test_select_where_string(col):
    c = col.execute("SELECT * FROM {}.{} WHERE count_str = '42';".format(
        database_name, collection_name))
    assert len(c) == 1
    c.close()


def test_select_order_by_asc(col):
    c = col.execute("SELECT * FROM {}.{} ORDER BY count ASC;".format(
        database_name, collection_name))
    assert len(c) == 100
    c.next()
    assert c['count'] == 0
    c.next()
    assert c['count'] == 1
    c.close()


def test_select_order_by_desc(col):
    c = col.execute("SELECT * FROM {}.{} ORDER BY count DESC;".format(
        database_name, collection_name))
    assert len(c) == 100
    c.next()
    assert c['count'] == 99
    c.next()
    assert c['count'] == 98
    c.close()


def test_delete_where(col):
    c = col.execute("DELETE FROM {}.{} WHERE count > 90;".format(database_name, collection_name))
    assert len(c) == 9
    c.close()

    c = col.execute("SELECT * FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 91
    c.close()


def test_delete_all(col):
    c = col.execute("DELETE FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 100
    c.close()

    c = col.execute("SELECT * FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 0
    c.close()


def test_update_set(col):
    c = col.execute("UPDATE {}.{} SET count = 1000 WHERE count < 10;".format(
        database_name, collection_name))
    assert len(c) == 10
    c.close()

    c = col.execute("SELECT * FROM {}.{} WHERE count = 1000;".format(
        database_name, collection_name))
    assert len(c) == 10
    c.close()

    c = col.execute("SELECT * FROM {}.{} WHERE count < 10;".format(
        database_name, collection_name))
    assert len(c) == 0
    c.close()


def test_update_set_string(col):
    c = col.execute("UPDATE {}.{} SET count_str = 'updated' WHERE count = 50;".format(
        database_name, collection_name))
    assert len(c) == 1
    c.close()

    c = col.execute("SELECT * FROM {}.{} WHERE count_str = 'updated';".format(
        database_name, collection_name))
    assert len(c) == 1
    c.close()


def test_insert_additional_rows(col):
    query = "INSERT INTO {}.{} (_id, count, count_str, countFloat, count_bool) VALUES ".format(
        database_name, collection_name)
    rows = []
    for num in range(100, 110):
        _id = gen_id(num)
        rows.append("('{}', {}, '{}', {}, TRUE)".format(_id, num, str(num), num + 0.1))
    query += ", ".join(rows) + ";"
    c = col.execute(query)
    assert len(c) == 10
    c.close()

    c = col.execute("SELECT * FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 110
    c.close()


def test_duplicate_id_not_inserted(col):
    query = "INSERT INTO {}.{} (_id, count) VALUES ('{}', 9999);".format(
        database_name, collection_name, gen_id(0))
    c = col.execute(query)
    assert len(c) == 0
    c.close()

    c = col.execute("SELECT * FROM {}.{};".format(database_name, collection_name))
    assert len(c) == 100
    c.close()