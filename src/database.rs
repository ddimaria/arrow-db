use std::fmt::Debug;

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use datafusion::prelude::SessionContext;
use tokio::fs;

use crate::{
    error::{DbError, Result},
    table::Table,
};

const DISK_PATH: &'static str = "./data/";

#[derive(Clone)]
pub struct Database<'a> {
    pub name: &'a str,
    pub tables: DashMap<&'a str, Table<'a>>,
    pub ctx: SessionContext,
}

impl Debug for Database<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .field("tables", &self.tables)
            .finish()
    }
}

impl<'a> Database<'a> {
    pub fn new(name: &'a str) -> Result<Database<'a>> {
        if name.contains(" ") {
            return Err(DbError::CreateDatabase(
                "Database name cannot contain spaces".into(),
            ));
        }

        Ok(Database {
            name,
            tables: DashMap::new(),
            ctx: SessionContext::new(),
        })
    }

    /// Add a table to the database
    pub fn add_table(&mut self, table: Table<'a>) -> Result<()> {
        let table_name = table.name;

        if self.tables.contains_key(table_name) {
            return Err(DbError::TableAlreadyExists(table_name.into()));
        }

        self.tables.insert(table_name, table);

        Ok(())
    }

    /// Get a table from the database
    pub fn get_table(&self, name: &str) -> Result<Ref<'a, &str, Table>> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.into()))
    }

    /// Get a mutable table from the database
    pub fn get_mut_table(&self, name: &str) -> Result<RefMut<'a, &str, Table>> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.into()))
    }

    /// Create a new database from a directory on disk
    ///
    /// The directory name is the database name, and each file
    /// within the directory is a parquet file representing a table
    pub async fn new_from_disk(name: &str) -> Result<Database> {
        let mut database = Database::new(name)?;
        let path = format!("{DISK_PATH}{}", database.name);
        let mut entries = fs::read_dir(path.to_owned()).await.map_err(|e| {
            DbError::CreateDatabase(format!("Error reading file: {}", e.to_string()))
        })?;

        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_file() {
                    let file_name = entry.file_name();
                    let file_str = file_name.to_string_lossy();

                    if let Some((table_name, extension)) = file_str.split_once('.') {
                        if extension != "parquet" {
                            continue;
                        }

                        let table_name = Box::new(table_name.to_string());
                        let mut table = Table::new(Box::leak(table_name.clone()));

                        table.import_parquet_from_disk(&path).await?;
                        database.add_table(table)?;
                    }
                }
            }
        }

        Ok(database)
    }

    /// Export the database to a directory on disk
    pub async fn export_to_disk(&self) -> Result<()> {
        let path = format!("{DISK_PATH}{}", self.name);
        fs::create_dir_all(path.to_owned()).await.map_err(|e| {
            DbError::CreateDatabase(format!("Error creating directory: {}", e.to_string()))
        })?;

        for table in self.tables.iter() {
            table
                .value()
                .to_owned()
                .export_parquet_to_disk(&path)
                .await?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn print(&self) {
        for table in self.tables.iter() {
            println!("\nDatabase: {}", self.name);
            table.value().print();
        }
    }
}

#[macro_export]
macro_rules! get_table {
    ( $self:ident, $name:tt ) => {
        $self
            .tables
            .get(&$name)
            .ok_or($crate::error::DbError::TableNotFound($name.into()))
    };
}

#[macro_export]
macro_rules! get_mut_table {
    ( $self:ident, $name:tt ) => {
        $self
            .tables
            .get_mut(&$name)
            .ok_or($crate::error::DbError::TableNotFound($name.into()))
    };
}

#[cfg(test)]
pub mod tests {

    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;
    use std::time::Instant;

    use super::*;

    pub fn create_database<'a>() -> (Database<'a>, Table<'a>) {
        let mut database = Database::new("MyDB").unwrap();

        let table_users = Table::new("users");
        database.add_table(table_users.clone()).unwrap();

        let table_user_role = Table::new("user_role");
        database.add_table(table_user_role.clone()).unwrap();

        (database, table_users)
    }

    pub fn seed_database<'a>(database: &mut Database) {
        get_mut_table!(database, "users")
            .unwrap()
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3, 4]).into(),
            )
            .unwrap();

        get_mut_table!(database, "users")
            .unwrap()
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        get_mut_table!(database, "user_role")
            .unwrap()
            .add_column::<Int32Array>(
                0,
                "user_id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3, 4]).into(),
            )
            .unwrap();

        get_mut_table!(database, "user_role")
            .unwrap()
            .add_column::<StringArray>(
                1,
                "role",
                DataType::Utf8,
                StringArray::from(vec!["admin", "manager", "employee", "employee"]).into(),
            )
            .unwrap();
    }

    #[test]
    fn test_database_and_table_creation() {
        let (mut database, table) = create_database();
        seed_database(&mut database);

        // expect an error when adding the same table
        assert_eq!(
            database.add_table(table.clone()),
            Err(DbError::TableAlreadyExists("users".into()))
        );

        let table_ref = database.tables.get("users").unwrap().clone();
        assert_eq!(table_ref.name, table.name);

        assert!(database.tables.get("non_existent_table").is_none());

        database.print();
    }

    #[tokio::test]
    async fn test_database_new_from_disk() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.export_to_disk().await.unwrap();

        let _database = Database::new_from_disk(database.name).await.unwrap();
    }

    #[tokio::test]
    async fn text_benchmark_large_db() {
        let now = Instant::now();
        let database = Database::new_from_disk("LargeDB").await.unwrap();
        let elapsed = now.elapsed();

        let rows = get_table!(database, "flights_1m")
            .unwrap()
            .record_batch
            .num_rows();
        let cols = get_table!(database, "flights_1m")
            .unwrap()
            .record_batch
            .num_columns();

        println!("Loaded {} rows and {} cols in {:.2?}", rows, cols, elapsed);

        let now = Instant::now();
        database.export_to_disk().await.unwrap();
        let elapsed = now.elapsed();

        println!(
            "Exported {} rows and {} cols in {:.2?}",
            rows, cols, elapsed
        );
    }
}
