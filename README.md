# Arrow DB

A Rust workspace for an Arrow-based database built on top of Arrow Flight SQL.

## Workspace Members

| Crate                                        | Description                                                                                |
| -------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [arrow-db-core](arrow-db-core/README.md)     | The core logic for Arrow DB.                                                               |
| [arrow-db-server](arrow-db-server/README.md) | A Tonic server that uses the Arrow Flight SQL protocol and DataFusion for query execution. |
| [arrow-db-client](arrow-db-client/README.md) | A Rust client for Arrow DB.                                                                |
