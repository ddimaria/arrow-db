# Arrow DB Core

This is the core library for Arrow DB.  It contains the logic for the Arrow DB server.

## In Memory Storage

Arrow DB is a thin wrapper around Arrow's RecordBatch and DataFusion's SessionContext.

## Disk Storage

Arrow DB serializes the database tables into Parquet files on disk.  This allows for persisting data after the server is shutdown.

## Usage

### Create a Database

Create a new database.

```rust
use arrow_db_core::database::Database;

let database = Database::new("MyDB")?;
```

### Create a Table

Create a new table in the database.

```rust
use arrow_db_core::database::Database;
use arrow_db_core::table::Table;

let database = Database::new("MyDB")?;
let table = Table::new("users");
database.add_table(table)?;
```

### Add a Column and Data to a Table

Add a column to the table and populate it with data.

```rust
use arrow_db_core::database::Database;
use arrow_db_core::table::Table;
use arrow::array::{Int32Array, StringArray};

let database = Database::new("MyDB")?;
let table = Table::new("users");
database.add_table(table)?;

get_mut_table!(database, "users")?
    .add_column::<Int32Array>(
        0,                                          // column index 
        "id",                                       // name
        DataType::Int32,                            // data type
        Int32Array::from(vec![1, 2, 3, 4]).into(),  // data
    )?;
```

### Append Data to a Column

Append data to an existing column.

```rust
get_mut_table!(database, "users")?
    .append_column_data::<Int32Array>(
        0,                                // column index
        Int32Array::from(vec![3]).into(), // data
    )?;
```

### Insert Data into a Column at a Specific Row

Insert data into a column at a specific row.

```rust
get_mut_table!(database, "users")?
    .insert_column_data::<Int32Array>(
        0,                                // column index
        2,                                // row index
        Int32Array::from(vec![4]).into(), // data
    )?;
```

### Update Data in a Column at a Specific Row

Update data in a column at a specific row.

```rust
get_mut_table!(database, "users")?
    .update_column_data::<Int32Array>(
        0,                                // column index
        2,                                // row index
        Int32Array::from(vec![5]).into(), // data
    )?;
```

### Delete a Column

Delete a column from the table.

```rust
get_mut_table!(database, "users")?.delete_column(0)?;
```
