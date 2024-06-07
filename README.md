# harlequin-cassandra

> [!CAUTION]
> This adapter is unstable and experimental.
>
> Proceed with caution!

> [!WARNING]
> This adapter does not aim to support [Scylla](https://www.scylladb.com).

> [!NOTE]
> This adapter currently does not support execution profiles, load-balancing
> polices, and consistency levels.

This is a [Cassandra](http://cassandra.apache.org) adapter for [Harlequin](https://harlequin.sh).  
It is based on [Datastax' cassandra-driver](https://github.com/datastax/python-driver).

## Integration With Harlequin

Cassandra doesn't use cursor(s), thus `HarlequinCursor` and `HarlequinConnection`
behave differently in this adapter.

A manual translation of `cassandra-driver` objects types to Python types is
required for Apache Arrow to work correctly.

**Some quirks are to be expected.**

## Things To Resolve

A list of things to resolve before marking this adapter as "stable".

- [x] Debug issues raised by Arrow during certain `SELECT` statements.
- [x] Make catalog faster.
- [x] Add views to catalog.
- [x] Add test(s) that create keyspaces, tables.
- [x] Add an option to set the protocol level manually.
- [ ] Add an option to support execution profiles.
- [ ] Add an option to support load-balancing policies.
- [ ] Add an option to support consistency levels.
