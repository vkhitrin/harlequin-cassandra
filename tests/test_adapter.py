import os
import sys
from collections.abc import Generator
from types import NoneType
from uuid import uuid4

import pytest
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
    "username": os.environ.get("HARLEQUIN_CASSANDRA_TEST_USERNAME")
    if os.environ.get("HARLEQUIN_CASSANDRA_TEST_USERNAME")
    else "cassandra",
    "password": os.environ.get("HARLEQUIN_CASSANDRA_TEST_PASSWORD")
    if os.environ.get("HARLEQUIN_CASSANDRA_TEST_PASSWORD")
    else "cassandra",
}
TEST_CONNECTION_OPTIONS = {
    "host": os.environ.get("HARLEQUIN_CASSANDRA_TEST_HOST")
    if os.environ.get("HARLEQUIN_CASSANDRA_TEST_HOST")
    else "localhost",
    "port": os.environ.get("HARLEQUIN_CASSANDRA_PORT")
    if os.environ.get("HARLEQUIN_CASSANDRA_PORT")
    else "9042",
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


@pytest.fixture
def setup_and_teradown_keyspace(connection: HarlequinCassandraConnection) -> Generator:
    session = connection.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test
        WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
        """
    )
    assert isinstance(session, HarlequinCursor)
    session.fetchall()
    try:
        yield
    finally:
        session = connection.execute("DROP KEYSPACE IF EXISTS test")
        assert isinstance(session, HarlequinCursor)
        session.fetchall()


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
    session = connection.execute("SELECT keyspace_name FROM system_schema.tables;")
    assert isinstance(session, HarlequinCursor)
    session = session.set_limit(2)
    assert isinstance(session, HarlequinCursor)
    data = session.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinCassandraConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")


@pytest.mark.usefixtures("setup_and_teradown_keyspace")
def test_create_keyspace(connection: HarlequinCassandraConnection) -> None:
    assert connection.cluster.metadata.keyspaces.get("test")


@pytest.mark.usefixtures("setup_and_teradown_keyspace")
def test_create_table(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute(
        """
        CREATE TABLE IF NOT EXISTS test.mocktable (
            id UUID PRIMARY KEY,
            name text,
            position int);
        """
    )
    data = session.fetchall()
    assert isinstance(data, NoneType)


@pytest.mark.usefixtures("setup_and_teradown_keyspace")
def test_insert_into_table(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute(
        """
        CREATE TABLE IF NOT EXISTS test.mocktable (
            id UUID PRIMARY KEY,
            name text,
            position int);
        """
    )
    session.fetchall()
    session = connection.execute(
        f"""
        INSERT INTO test.mocktable (
            id,name, position)
        VALUES ({uuid4()}, 'test', 1)
        IF NOT EXISTS;
        """
    )
    session.fetchall()
    session = connection.execute("SELECT * FROM test.mocktable;")
    data = session.fetchall()
    assert len(session.columns()) == 3
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


@pytest.mark.usefixtures("setup_and_teradown_keyspace")
def test_create_view(connection: HarlequinCassandraConnection) -> None:
    session = connection.execute(
        """
        CREATE TABLE IF NOT EXISTS test.mocktable (
            id UUID PRIMARY KEY,
            name text,
            position int);
        """
    )
    session.fetchall()
    session = connection.execute(
        f"""
        INSERT INTO test.mocktable (
            id,name, position)
        VALUES ({uuid4()}, 'test', 1)
        IF NOT EXISTS;
        """
    )
    session.fetchall()
    session = connection.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS test.mockview
            AS SELECT id, name, position
        FROM test.mocktable
            WHERE name IS NOT NULL AND position IS NOT NULL AND id IS NOT NULL
        PRIMARY KEY (id);
        """
    )
    session.fetchall()
    session = connection.execute("SELECT * FROM test.mockview;")
    data = session.fetchall()
    assert len(session.columns()) == 3
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1
