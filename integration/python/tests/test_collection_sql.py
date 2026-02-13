import os
import pytest
from otterbrix import Client

client = Client(os.getcwd() + "/test_collection_sql")
client.execute("CREATE DATABASE schema;")
client.execute("CREATE TABLE schema.table();")


def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res

def test_collection_sql():
    # insert

    query = "INSERT INTO schema.table (_id, name, count) VALUES "
    for num in  range(0, 100):
        query += "('" + gen_id(num + 1) + "', 'Name " + str(num) + "', " + str(num) + ")"
        if num == 99:
            query += ";"
        else:
            query += ", "
    c = client.execute(query)
    assert len(c) == 100
    c.close()

    # select

    c = client.execute("SELECT * FROM schema.table;")
    assert len(c) == 100
    c.close()

    # delete

    c = client.execute("SELECT * FROM schema.table WHERE count > 90;")
    assert len(c) == 9
    c.close()

    c = client.execute("DELETE FROM schema.table WHERE count > 90;")
    assert len(c) == 9
    c.close()

    c = client.execute("SELECT * FROM schema.table WHERE count > 90;")
    assert len(c) == 0
    c.close()

    c = client.execute("SELECT * FROM schema.table;")
    assert len(c) == 91
    c.close()

    # update

    c = client.execute("SELECT * FROM schema.table WHERE count < 20;")
    assert len(c) == 20
    c.close()

    c = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
    assert len(c) == 0
    c.close()

    c = client.execute("UPDATE schema.table SET count = 1000 WHERE count < 20;")
    assert len(c) == 20
    c.close()

    c = client.execute("SELECT * FROM schema.table WHERE count < 20;")
    assert len(c) == 0
    c.close()

    c = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
    assert len(c) == 20
    c.close()