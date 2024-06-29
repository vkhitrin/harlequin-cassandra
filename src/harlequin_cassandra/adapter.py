from __future__ import annotations

from datetime import date
from itertools import cycle
from typing import Any

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    Cluster,
    ConsistencyLevel,
    PreparedStatement,
    Session,
)
from cassandra.cqltypes import CassandraType
from cassandra.metadata import KeyspaceMetadata
from cassandra.protocol import SyntaxException
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
    HarlequinTransactionMode,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin.options import HarlequinAdapterOption
from textual_fastdatatable.backend import AutoBackendType

from harlequin_cassandra.cli_options import CASSANDRA_OPTIONS
from harlequin_cassandra.completions import _get_completions


class HarlequinCassandraCursor(HarlequinCursor):
    def __init__(
        self, conn: HarlequinCassandraConnection, statement: PreparedStatement
    ) -> None:
        self.conn = conn
        self.statement = statement
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
            self.data = self.conn.conn.execute(self.statement)
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
        conn: Session,
        cluster: Cluster,
        init_message: str = "",
    ) -> None:
        self.conn = conn
        self.init_message = init_message
        self.cluster = cluster

        # NOTE: (vkhitrin) label is limitted to 10 characters,
        #       if it's longer, it will not be displayed
        self._transaction_modes: list[HarlequinTransactionMode | None] = [
            HarlequinTransactionMode(label=level[:10])
            for level in ConsistencyLevel.name_to_value
        ]
        self._transaction_mode_gen = cycle(self._transaction_modes)
        self._transaction_mode = next(self._transaction_mode_gen)

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            statement: PreparedStatement = self.conn.prepare(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while preparing your query.",
            ) from e
        return HarlequinCassandraCursor(self, statement)

    def validate_sql(self, query: str) -> str:
        try:
            self.conn.prepare(query)
        except SyntaxException:
            return ""
        return query

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
            views: list[str] = list(keyspaces_metadata.get(keyspace).views.keys())  # type:  ignore
            table_items: list[CatalogItem] = []
            view_items: list[CatalogItem] = []
            for table in tables:
                table_column_items: list[CatalogItem] = []
                table_columns: list[str] = list(
                    keyspaces_metadata.get(keyspace).tables.get(table).columns.keys()  # type: ignore
                )
                for column in table_columns:
                    column_type = (
                        keyspaces_metadata.get(keyspace)  # type:ignore
                        .tables.get(table)
                        .columns.get(column)
                        .cql_type
                    )
                    table_column_items.append(
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
                        children=table_column_items,
                    )
                )
                for view in views:
                    view_column_items: list[CatalogItem] = []
                    view_columns: list[str] = list(
                        keyspaces_metadata.get(keyspace).views.get(view).columns.keys()  # type: ignore
                    )
                    for column in view_columns:
                        column_type = (
                            keyspaces_metadata.get(keyspace)  # type:ignore
                            .views.get(view)
                            .columns.get(column)
                            .cql_type
                        )
                        view_column_items.append(
                            CatalogItem(
                                qualified_identifier=f'"{keyspace}"."{view}"."{column}"',
                                query_name=f'"{keyspace}"."{view}"."{column}"',
                                label=column,
                                type_label=self._get_short_type_from_column_type(
                                    column_type
                                ),
                            )
                        )
                    view_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{keyspace}"."{view}"',
                            query_name=f'"{keyspace}"."{view}"',
                            label=view,
                            type_label="v",
                            children=view_column_items,
                        )
                    )

            keyspace_items.append(
                CatalogItem(
                    qualified_identifier=f'"{keyspace}"',
                    query_name=f'"{keyspace}"',
                    label=keyspace,
                    type_label="ks",
                    children=table_items + view_items,
                )
            )
        return Catalog(items=keyspace_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        completions = _get_completions()
        return completions
        ...

    @property
    def transaction_mode(self) -> HarlequinTransactionMode | None:
        consistency_level = self.conn.default_consistency_level
        for t_mode in self._transaction_modes:
            if (
                t_mode.label
                == ConsistencyLevel.value_to_name.get(consistency_level)[:10]
            ):
                return t_mode

    def toggle_transaction_mode(self) -> HarlequinTransactionMode | None:
        new_mode = next(self._transaction_mode_gen)
        self._transaction_mode = new_mode
        self._sync_connection_transaction_mode()
        return new_mode

    def _sync_connection_transaction_mode(self) -> None:
        logical_level_name = self._transaction_mode.label
        # NOTE: (vkhitrin): Workaround due to the limitation of partial
        #       labels for long consistency level names
        if logical_level_name == "LOCAL_QUOR":
            logical_level_name = "LOCAL_QUORUM"
        elif logical_level_name == "EACH_QUORU":
            logical_level_name = "EACH_QUORUM"
        elif logical_level_name == "LOCAL_SERI":
            logical_level_name = "LOCAL_SERIAL"

        new_consistency_level = ConsistencyLevel.name_to_value.get(logical_level_name)
        self.conn.default_consistency_level = new_consistency_level
        return

    def close(self) -> None:
        self.cluster.shutdown()


class HarlequinCassandraAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS: list[HarlequinAdapterOption] = CASSANDRA_OPTIONS

    # NOTE: (vkhitrin) this is most likely not the correct way
    #       to apply defaults. Without this explicit definition
    #       defaults are not applied.
    def __init__(
        self,
        host: str = CASSANDRA_OPTIONS[0].default,
        port: str = CASSANDRA_OPTIONS[1].default,
        keyspace: str = CASSANDRA_OPTIONS[2].default,
        user: str | None = None
        if not CASSANDRA_OPTIONS[3].default
        else CASSANDRA_OPTIONS[3].default,
        password: str | None = None
        if not CASSANDRA_OPTIONS[4].default
        else CASSANDRA_OPTIONS[4].default,
        protocol_version: int = CASSANDRA_OPTIONS[5].default,
        consistency_level: str = CASSANDRA_OPTIONS[6].default,
        **_: Any,
    ) -> None:
        self.auth_options = {
            "username": user,
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
        self.consistency_level = consistency_level

    # TODO: (vkhitrin) should be revisited in the future.
    #       Iterrate and work on mapping Cassandra objects to Arrow.
    #       There might be a benefit in attempting to convert values
    #       directly to Arrow types.
    @staticmethod
    def _cassandra_to_py_factory(column_names: list[str], rows: list[Any]) -> Any:
        """Method to ensure that all values from Cassandra are returned as matching
        pyarrow objects.
        """
        CASSANDRA_TYPES_TO_PYTHON: dict[str, Any] = {
            "UUID": str,
            "SortedSet": list,
            "MapType": dict,
            "SetType": list,
            "TupleType": tuple,
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

        return [tuple(cass_to_py(row)) for row in rows]

    def connect(self) -> HarlequinCassandraConnection:
        try:
            auth_provider = PlainTextAuthProvider(**self.auth_options)
            self.cluster = Cluster(**self.options, auth_provider=auth_provider)
            conn = self.cluster.connect(**self.connection_options)
            conn.row_factory = self._cassandra_to_py_factory
            conn.default_consistency_level = ConsistencyLevel.name_to_value.get(
                self.consistency_level
            )
        except Exception as e:
            raise HarlequinConnectionError(
                msg=f"Excpetion: {e.__class__}, Message: {e}",
                title="Harlequin could not connect to a Cassandra Cluster.",
            ) from e
        return HarlequinCassandraConnection(
            conn=conn, cluster=self.cluster, init_message="Connected to a Cassandra Cluster."
        )
