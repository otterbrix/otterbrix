import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.catalog import Table, Database, Column


class TestSparkCatalog(object):    

    def test_list_tables(self, spark):
        # empty
        tbls = spark.catalog.listTables()
        assert tbls == []

        spark.conn.execute("create database test_db")
        spark.conn.execute('create table test_db.tbl()')
        tbls = spark.catalog.listTables()
        print(tbls)
        tbl = Table(
                name='tbl',
                database='test_db',
                description='',
                tableType='',
                isTemporary=False,
            )
        assert tbl in tbls 
