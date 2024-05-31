import sys

import pytest
import pdb
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin_cassandra.adapter import (
    HarlequinCassandraAdapter,
    HarlequinCassandraConnection,
)
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

TEST_AUTH_OPTIONS = {
    "username": "cassandra",
    "password": "cassandra",
}
TEST_CONNECTION_OPTIONS = {
    "host": "localhost",
    "port": "9042",
}


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "cassandra"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinCassandraAdapter


def test_connect() -> None:
    conn = HarlequinCassandraAdapter(
        **TEST_AUTH_OPTIONS, **TEST_CONNECTION_OPTIONS
    ).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs() -> None:
    assert HarlequinCassandraAdapter(
        **TEST_AUTH_OPTIONS, **TEST_CONNECTION_OPTIONS, keyspace="system"
    ).connect()


def test_connect_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinCassandraAdapter(host="foo").connect()


@pytest.fixture
def connection() -> HarlequinCassandraConnection:
    return HarlequinCassandraAdapter(
        **TEST_AUTH_OPTIONS, **TEST_CONNECTION_OPTIONS, keyspace="system"
    ).connect()


def test_get_catalog(connection: HarlequinCassandraConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_select(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute("SELECT key from system.local")
    assert isinstance(session, HarlequinCursor)
    data = session.fetchall()
    assert session.columns() == [("key", "s")]
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_dupe_cols(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute(
        "SELECT key AS a, key AS b, key AS c FROM system.local;"
    )
    assert isinstance(session, HarlequinCursor)
    data = session.fetchall()
    assert len(session.columns()) == 3
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute(
        "SELECT keyspace_name FROM system_schema.tables;"
    )
    assert isinstance(session, HarlequinCursor)
    session = session.set_limit(2)
    assert isinstance(session, HarlequinCursor)
    data = session.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute("selec;")
    with pytest.raises(HarlequinQueryError):
        _ = session.fetchall()
