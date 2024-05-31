from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

host = TextOption(
    name="host",
    description=(
        "Specifies the host name of the machine on which the server is running. "
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
    short_decls=["-k", "--keyspace"],
)


user = TextOption(
    name="username",
    description=("Cassandra user name to connect as."),
    short_decls=["-u", "--user", "-U"],
)


password = TextOption(
    name="password",
    description=("Password to be used if the server demands password authentication."),
)

cassandra_OPTIONS = [
    host,
    port,
    keyspace,
    user,
    password
]
