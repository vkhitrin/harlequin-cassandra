from __future__ import annotations

from datetime import date
from typing import Any

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ResultSet, Session
from cassandra.cqltypes import CassandraType
from cassandra.metadata import KeyspaceMetadata
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
        names = self.data.column_names if self.data.column_names else ()
        types = (
            [
                self.conn._get_short_type_from_cassandra_class(column_type)
                for column_type in self.data.column_types
            ]
            if self.data.column_types
            else ()
        )
        return list(zip(names, types))

    def set_limit(self, limit: int) -> HarlequinCassandraCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        result: tuple[list[Any]] | None = None
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
                limited_result: tuple = ()
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
        connection_options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        try:
            auth_provider = PlainTextAuthProvider(**auth_options)
            self.cluster = Cluster(**options, auth_provider=auth_provider)
            self.session = self.cluster.connect(**connection_options)
            self.session.row_factory = self.cassandra_to_py_factory
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

    # TODO: (vkhitrin) should be revisited in the future.
    #       Iterrate and work on mapping Cassandra objects to Arrow.
    #       There might be a benefit in attempting to convert values
    #       directly to Arrow types.
    @staticmethod
    def cassandra_to_py_factory(column_names: list[str], rows: list[Any]) -> Any:
        """Method to ensure that all values from Cassandra are returned as matching
        pyarrow objects.
        """
        CASSANDRA_TYPES_TO_PYTHON: dict[str, Any] = {
            "UUID": str,
            "SortedSet": list,
            "MapType": dict,
            "SetType": list,
            "TupleType": list,
            "VarcharType": str,
            "DateType": date,
            "TimeType": date,
            "TimestampType": date,
            "AsciiType": str,
            "BytesType": bytes,
            "UTF8Type": str,
            "BooleanType": bool,
            "DecimalType": int,
            "DoubleType": int,
            "FloatType": int,
            "Int32Type": int,
            "LongType": int,
            "UUIDType": str,
            "TimeUUIDType": str,
            "UserType": dict,  # Most likely should be some kind of struct
            "InetAddressType": str,
        }

        def cass_to_py(row: Any) -> Any:
            return [
                CASSANDRA_TYPES_TO_PYTHON.get(type(value).__name__)(value)
                if type(value).__name__ in CASSANDRA_TYPES_TO_PYTHON
                else str(value)
                for value in row
            ]

        return [cass_to_py(row) for row in rows]

    @staticmethod
    def _get_short_type_from_cassandra_class(cassandra_type: CassandraType) -> str:
        MAPPING = {
            "ListType": "[]",
            "SortedSet": "[]",
            "MapType": "{}",
            "SetType": "[]",
            "TupleType": "()",
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
            "UserType": "ut",
            "InetAddressType": "ip",
        }
        cass_type_name = cassandra_type.__name__
        if cassandra_type in MAPPING:
            return MAPPING[cass_type_name]

        for map_type in MAPPING:
            if map_type in cass_type_name:
                return MAPPING[map_type]
        return "?"

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
            "duration": "str",  # Should be mapped to Arrow's duration
            "float": "#.#",
            "inet": "ip",
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
        keyspaces_metadata: dict[str, KeyspaceMetadata] = (
            self.cluster.metadata.keyspaces
        )
        keyspace_items: list[CatalogItem] = []
        for keyspace in keyspaces_metadata:
            tables: list[str] = list(keyspaces_metadata.get(keyspace).tables.keys())  # type: ignore
            table_items: list[CatalogItem] = []
            for table in tables:
                column_items: list[CatalogItem] = []
                columns: list[str] = list(
                    keyspaces_metadata.get(keyspace).tables.get(table).columns.keys()  # type: ignore
                )
                for column in columns:
                    column_type = (
                        keyspaces_metadata.get(keyspace)  # type:ignore
                        .tables.get(table)
                        .columns.get(column)
                        .cql_type
                    )
                    column_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{keyspace}"."{table}"."{column}"',
                            query_name=f'"{keyspace}"."{table}"."{column}"',
                            label=column,
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
        protocol_version: int | None = None,
        **_: Any,
    ) -> None:
        self.auth_options = {
            "username": username,
            "password": password,
        }
        self.options: dict[str, Any] = {
            "contact_points": [host],
            "port": port,
        }
        self.connection_options: dict[str, Any] = {}
        if protocol_version:
            self.options["protocol_version"] = int(protocol_version)
        if keyspace:
            self.connection_options["keyspace"] = keyspace

    def connect(self) -> HarlequinCassandraConnection:
        conn = HarlequinCassandraConnection(
            auth_options=self.auth_options,
            options=self.options,
            connection_options=self.connection_options,
            init_message="Connected to Cassandra",
        )
        return conn
