from typing import List, NamedTuple, Optional
from .session import SparkSession


class Database(NamedTuple):
    name: str
    description: Optional[str]
    locationUri: str


class Table(NamedTuple):
    name: str
    database: Optional[str]
    description: Optional[str]
    tableType: str
    isTemporary: bool


class Column(NamedTuple):
    name: str
    description: Optional[str]
    dataType: str
    nullable: bool
    isPartition: bool
    isBucket: bool


class Function(NamedTuple):
    name: str
    description: Optional[str]
    className: str
    isTemporary: bool


class Catalog:
    def __init__(self, session: SparkSession):
        self._session = session

    def listDatabases(self) -> List[Database]:
        #res = self._session.conn.sql('select database_name from otterbrix_databases()').fetchall()

        def transform_to_database(x) -> Database:
            return Database(name=x[0], description=None, locationUri='')

        #databases = [transform_to_database(x) for x in res]
        #return databases
        raise NotImplementedError

    def listTables(self) -> List[Table]:
        res = self._session.conn.listTables()
        def transform_to_table(x) -> Table:
            db, table = None, None
            parts = x.rsplit('.', 1)
            if len(parts) == 1:
                db, table = parts[0], ""
            else:
                db, table = parts
            return Table(name=table, database=db, description="", tableType='', isTemporary=False)

        tables = [transform_to_table(x) for x in res]
        return tables
        raise NotImplementedError

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[Column]:
        query = f"""
			select column_name, data_type, is_nullable from otterbrix_columns() where table_name = '{tableName}'
		"""
        if dbName:
            query += f" and database_name = '{dbName}'"
        #res = self._session.conn.sql(query).fetchall()

        def transform_to_column(x) -> Column:
            return Column(name=x[0], description=None, dataType=x[1], nullable=x[2], isPartition=False, isBucket=False)

        #columns = [transform_to_column(x) for x in res]
        #return columns
        raise NotImplementedError

    def listFunctions(self, dbName: Optional[str] = None) -> List[Function]:
        raise NotImplementedError

    def setCurrentDatabase(self, dbName: str) -> None:
        raise NotImplementedError


__all__ = ["Catalog", "Table", "Column", "Function", "Database"]
