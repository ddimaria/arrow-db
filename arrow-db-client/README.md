# Arrow DB Client

This is a client for the Arrow DB server. It allows you to execute SQL queries and receive results in Arrow format.

## Usage

### Create a client

Create a client with the Arrow DB server URL.  This client will be used to get the schema and execute queries.

```rust
use arrow_db_client::DbClient;

let client = DbClient::new("http://localhost:50051").await.unwrap();
```

### Get the schema

Get the schema of the Arrow DB.  This is useful to understand the data types and shapes of the data.

```rust

use arrow_db_client::DbClient;

let client = DbClient::new("http://localhost:50051").await.unwrap();
let schema = client.schema().await.unwrap();
```

### Execute a query

Execute a query and receive the results as a vector of Arrow RecordBatches.
```rust
use arrow_db_client::DbClient;

let client = DbClient::new("http://localhost:50051").await.unwrap();
let results = client.query("SELECT * FROM users").await.unwrap();
```
