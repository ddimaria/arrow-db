# ArrowDB

ArrowDB is a teaching tool for learning about the power of Arrow and Arrow tooling in the cloud and in the browser.  Rust is used as the primary programming language.

## Workspace Members

| Crate                                          | Description                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------- |
| [arrow-db-core](arrow-db-core/README.md)       | The core ArrowDB DB.                                                |
| [arrow-db-server](arrow-db-server/README.md)   | A Tonic server that leverages the Arrow Flight SQL protocol .       |
| [arrow-db-client](arrow-db-client/README.md)   | A Rust client for querying the ArrowDB server.                      |
| [arrow-db-wasm](arrow-db-wasm/README.md)       | A WebAssembly module for use in the ArrowDB browser.                |
| [arrow-db-browser](arrow-db-browser/README.md) | A React app for interacting with the ArrowDB server in the browser. |

## ArrowDB Fundamentals

ArrowDB is built on top of the [Apache Arrow](https://arrow.apache.org/) library in [Rust](https://docs.rs/arrow/latest/arrow/).  Arrow is a [columnar format](https://arrow.apache.org/docs/format/Columnar.html) that is optimized for in-memory data processing and analytics.  Full specifications for Arrow can be found at [https://arrow.apache.org/docs/format/index.html](https://arrow.apache.org/docs/format/index.html).

A good analog for database tables in Arrow is a [RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html).  A RecordBatch is a two-dimensional collection of column-oriented data that is defined by a [Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html).  The Schema defines the [Fields](https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html) in the RecordBatch, which act as columns in a database.  Each Field is a column of data of a single Array type.