from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
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
    default=["localhost"],
)

port = TextOption(
    name="port",
    description=("Port number to connect to at the server host."),
    short_decls=["-p"],
    default="9042",
)


keyspace = TextOption(
    name="keyspace",
    description=("The keyspace name to use when connecting with the Cassandra server."),
    short_decls=["-k"],
)


username = TextOption(
    name="username",
    description=("Cassandra user name to connect as."),
    short_decls=["-u"],
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

cassandra_OPTIONS = [host, port, keyspace, username, password, protocol_version]
