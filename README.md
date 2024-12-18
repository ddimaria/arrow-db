<!-- omit in toc -->
# ArrowDB

ArrowDB is a teaching tool for learning about the power of Arrow and Arrow tooling in the cloud and in the browser.  Rust is used as the primary programming language.

<!-- omit in toc -->
## Workspace Members

| Crate                                          | Description                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------- |
| [arrow-db-core](arrow-db-core/README.md)       | The core ArrowDB DB.                                                |
| [arrow-db-server](arrow-db-server/README.md)   | A Tonic server that leverages the Arrow Flight protocol .           |
| [arrow-db-client](arrow-db-client/README.md)   | A Rust client for querying the ArrowDB server.                      |
| [arrow-db-wasm](arrow-db-wasm/README.md)       | A WebAssembly module for use in the ArrowDB browser.                |
| [arrow-db-browser](arrow-db-browser/README.md) | A React app for interacting with the ArrowDB server in the browser. |

## ArrowDB Fundamentals

ArrowDB is built on top of the [Apache Arrow](https://arrow.apache.org/) library in [Rust](https://docs.rs/arrow/latest/arrow/).  Arrow is a [columnar format](https://arrow.apache.org/docs/format/Columnar.html) that is optimized for in-memory data processing and analytics.  Full specifications for Arrow can be found at [https://arrow.apache.org/docs/format/index.html](https://arrow.apache.org/docs/format/index.html).

A good analog for database tables in Arrow is a [RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html).  A RecordBatch is a two-dimensional collection of column-oriented data that is defined by a [Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html).  The Schema defines the [Fields](https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html) in the RecordBatch, which act as columns in a database.  Each Field is a column of data of a single Array type.

### Disk Persistence

ArrowDB uses the [Parquet format](https://parquet.apache.org/) for disk persistence.  Parquet is similar to Arrow, but is optimized for disk storage.  Parquet files can be read and written using the [Parquet crate](https://docs.rs/parquet/latest/parquet/).  Like RecordBatches in Arrow, Parquet files contain Row Groups.  Converting Arrow RecordBatches to Parquet files and vice-versa is time and space efficient.

### DataFusion

[DataFusion](https://docs.rs/datafusion/latest/datafusion/) is an extensible, parallel query execution engine built on top of Arrow.  DataFusion has a DataFrame and SQL API, though the SQL API is used in ArrowDB to support SQL-like queries.

### Arrow Flight RPC

[Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) is a protocol for exchanging streams of Arrow RecordBatches over the wire.  On the server side in Rust, Arrow Flight is implemented using [Tonic](https://docs.rs/tonic/latest/tonic/), which is a gRPC server framework.  gRPC uses Protocol Buffers (protobuf) to define the structure of the data and the service definition.  

On the client side in Rust, the [FlightServiceClient](https://docs.rs/arrow_flight/latest/arrow_flight/flight_service_client/struct.FlightServiceClient.html) is used to request and receive Arrow RecordBatches from the server.

### WebAssembly

[WebAssembly](https://webassembly.org/) (Wasm) is a bytecode format for the browser, though Wasm is not limited to browsers.  It is supported by all modern browsers and can be used to run Rust code in the browser.  Since browser Wasm doesn't have access to the file system, ArrowDB just exists in-memory within the browser.  The [arrow-db-wasm](arrow-db-wasm/README.md) crate contains Rust code for interacting with Arrow data in the browser.  The [arrow-db-browser](arrow-db-browser/README.md) app is a React app that uses the arrow-db-wasm crate to manipulate Arrow data in the browser.
