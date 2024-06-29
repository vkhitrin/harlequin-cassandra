from cassandra.cluster import ConsistencyLevel

from harlequin.options import (
    SelectOption,
    TextOption,
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert '{s}' to an int!"
    else:
        return True, ""


host = TextOption(
    name="host",
    description=(
        "Specifies the initial host to connect to. "
        "After the driver successfully connects to the node, it will auto discover"
        "the rest of the nodes in the cluster and will connect to them."
    ),
    short_decls=["-h"],
    default="localhost",
)

port = TextOption(
    name="port",
    description=("Port number to connect to at the server host."),
    short_decls=["-p"],
    default="9042",
    validator=_int_validator,
)


keyspace = TextOption(
    name="keyspace",
    description=("The keyspace name to use when connecting with the Cassandra server."),
    short_decls=["-k"],
)


user = TextOption(
    name="user",
    description=("Cassandra user name to connect as."),
    short_decls=["-u", "--username"],
)


password = TextOption(
    name="password",
    description=("Password to be used if the server demands password authentication."),
)

protocol_version = TextOption(
    name="protocol-version",
    description=(
        "The maximum version of the native protocol to use. "
        "If not specified, will be auto-discovered by the driver."
    ),
    short_decls=["-P"],
    validator=_int_validator,
)

consistency_level = SelectOption(
    name="consistency-level",
    description=(
        "Specifies how many replicas must respond for an operation to be considered a"
        "success. Default: `LOCAL_ONE`."
    ),
    short_decls=["-C"],
    choices=[
        "ANY",
        "ONE",
        "TWO",
        "THREE",
        "QUORUM",
        "ALL",
        "LOCAL_QUORUM",
        "EACH_QUORUM",
        "SERIAL",
        "LOCAL_SERIAL",
        "LOCAL_ONE",
    ],
    default="LOCAL_ONE",
)

CASSANDRA_OPTIONS = [
    host,
    port,
    keyspace,
    user,
    password,
    protocol_version,
    consistency_level,
]
