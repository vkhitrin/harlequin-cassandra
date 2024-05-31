from __future__ import annotations

from typing import Any, Sequence

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ResultSet, Session
from cassandra.cqltypes import CassandraType
from cassandra.protocol import SyntaxException
from cassandra.query import SimpleStatement
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_cassandra.cli_options import cassandra_OPTIONS
from harlequin_cassandra.completions import _get_completions


class HarlequinCassandraCursor(HarlequinCursor):
    def __init__(
        self,
        conn: HarlequinCassandraConnection,
        session: Session,
        statement: SimpleStatement,
    ) -> None:
        self.conn = conn
        self.session = session
        self.statement = statement
        self.data: ResultSet = None
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        names = self.data.column_names
        types = [
            self.conn._get_short_type_from_cassandra_class(column_type)
            for column_type in self.data.column_types
        ]
        return list(zip(names, types))

    def set_limit(self, limit: int) -> HarlequinCassandraCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        result: tuple(list) = tuple([])
        try:
            if self._limit:
                self.statement.fetch_size = self._limit
            self.data = self.session.execute(self.statement)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        if self.data:
            if self._limit:
                count = 0
                limited_result: tuple() = ()
                for row in self.data:
                    limited_result = limited_result + (row,)
                    count += 1
                    if count == self._limit:
                        break
                result = limited_result
            else:
                result = tuple([row for row in self.data])
        return result


class HarlequinCassandraConnection(HarlequinConnection):
    def __init__(
        self,
        *args: Any,
        init_message: str = "",
        auth_options: dict[str, Any],
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        try:
            auth_provider = PlainTextAuthProvider(**auth_options)
            self.cluster = Cluster(
                [options["host"]], port=options["port"], auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to Cassandra."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            statement = SimpleStatement(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while preparing your query.",
            ) from e
        return HarlequinCassandraCursor(self, self.session, statement)

    def validate_sql(self, query: str) -> str:
        try:
            self.session.prepare(query)
        except SyntaxException:
            return ""
        return query

    @staticmethod
    def _get_short_type_from_cassandra_class(c_type: CassandraType) -> str:
        MAPPING = {
            "VarcharType": "s",
            "DateType": "d",
            "TimeType": "s",
            "TimestampType": "s",
            "AsciiType": "s",
            "BytesType": "b",
            "UTF8Type": "s",
            "BooleanType": "t/f",
            "DecimalType": "#.#",
            "DoubleType": "#.#",
            "FloatType": "#.#",
            "Int32Type": "#",
            "LongType": "##",
            "UUIDType": "uuid",
            "TimeUUIDType": "uuid",
            "ListType": "[]",
            "MapType": "{}",
            "SetType": "[]",
            "TupleType": "()",
            "UserType": "ut",
        }
        return MAPPING.get(c_type.__name__, "?")

    @staticmethod
    def _get_short_type_from_column_type(col_type: str) -> str:
        MAPPING = {
            "ascii": "s",
            "bigint": "#",
            "blob": "blob",
            "boolean": "t/f",
            "counter": "#",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "duration": "duration",
            "float": "#.#",
            "inet": "string",
            "int": "#",
            "smallint": "#",
            "text": "s",
            "timestamp": "s",
            "timeuuid": "uuid",
            "tinyint": "#",
            "uuid": "uuid",
            "varchar": "s",
            "varint": "#",
        }
        return MAPPING.get(col_type, "?")

    def get_catalog(self) -> Catalog:
        keyspaces = self.cluster.metadata.keyspaces
        keyspace_items: list[CatalogItem] = []
        for keyspace in keyspaces:
            tables = self._get_tables(keyspace)
            table_items: list[CatalogItem] = []
            for table in tables:
                column_items: list[CatalogItem] = []
                columns = self._get_columns(keyspace, table)
                for column_name, column_type in columns:
                    column_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{keyspace}"."{table}"."{column_name}"',
                            query_name=f'"{keyspace}"."{table}"."{column_name}"',
                            label=column_name,
                            type_label=self._get_short_type_from_column_type(
                                column_type
                            ),
                        )
                    )
                table_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{keyspace}"."{table}"',
                        query_name=f'"{keyspace}"."{table}"',
                        label=table,
                        type_label="t",
                        children=column_items,
                    )
                )

            keyspace_items.append(
                CatalogItem(
                    qualified_identifier=f'"{keyspace}"',
                    query_name=f'"{keyspace}"',
                    label=keyspace,
                    type_label="ks",
                    children=table_items,
                )
            )
        return Catalog(items=keyspace_items)

    def _get_tables(self, keyspace_name: str) -> list[str]:
        tables_result = self.session.execute(
            f"""
            SELECT table_name 
            FROM system_schema.tables
            WHERE 
                keyspace_name='{keyspace_name}'
            ORDER BY table_name asc
            ;"""
        )
        return [table[0] for table in tables_result]

    def _get_columns(
        self, keyspace_name: str, table_name: str
    ) -> list[tuple[str, str]]:
        columns_result = self.session.execute(
            f"""
            SELECT column_name, type
            FROM system_schema.columns
            WHERE 
                keyspace_name='{keyspace_name}'
                AND table_name='{table_name}'
            ORDER BY column_name asc
            ;"""
        )
        return [(column[0], column[1]) for column in columns_result]

    def get_completions(self) -> list[HarlequinCompletion]:
        completions = _get_completions()
        return completions
        ...


class HarlequinCassandraAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = cassandra_OPTIONS

    def __init__(
        self,
        host: str | None = None,
        port: str | None = None,
        keyspace: str | None = None,
        username: str | None = None,
        password: str | None = None,
        **_: Any,
    ) -> None:
        self.auth_options = {
            "username": username,
            "password": password,
        }
        self.options = {
            "host": host,
            "port": port,
            "keyspace": keyspace,
        }

    def connect(self) -> HarlequinCassandraConnection:
        conn = HarlequinCassandraConnection(
            auth_options=self.auth_options, options=self.options
        )
        return conn
