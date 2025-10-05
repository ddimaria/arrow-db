//! Import operations.
//!
//! Tables can be imported from parquet files on disk.

use arrow::compute::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[cfg(not(target_arch = "wasm32"))]
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::error::{DbError, Result};
use crate::table::Table;

impl<'a> Table<'a> {
    /// Helper function to create a `DbError` for table import errors
    fn import_error(&self, error: impl ToString) -> DbError {
        DbError::TableImportError(self.name.into(), error.to_string())
    }

    /// Import the table from a parquet file on disk
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn import_parquet_from_disk(&mut self, path: &str) -> Result<()> {
        use futures::TryStreamExt;

        let file_name = format!("{path}/{}.parquet", self.name);
        let file = tokio::fs::File::open(&file_name)
            .await
            .map_err(|e| self.import_error(e))?;

        // self.import_parquet(file).await

        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| self.import_error(e))?
            .with_batch_size(8192);

        let stream = builder.build().map_err(|e| self.import_error(e))?;
        let record_batches = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| self.import_error(e))?;

        if let Some(batch) = record_batches.first() {
            let schema = batch.schema();
            self.record_batch =
                concat_batches(&schema, &record_batches).map_err(|e| self.import_error(e))?;
        }

        Ok(())
    }

    /// Import the table from a parquet file on disk
    pub fn import_parquet_from_bytes(&mut self, bytes: Bytes) -> Result<()> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| self.import_error(e))?
            .with_batch_size(8192);

        let reader = builder.build().map_err(|e| self.import_error(e))?;
        let record_batches = reader.flatten().collect::<Vec<_>>();

        if let Some(batch) = record_batches.first() {
            let schema = batch.schema();
            self.record_batch =
                concat_batches(&schema, &record_batches).map_err(|e| self.import_error(e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::test_utils::{create_temp_dir, remove_temp_dir};
    use crate::{database::tests::create_database, get_mut_table, get_table};

    #[tokio::test]
    async fn test_import_parquet_from_disk() {
        let (mut database, _) = create_database();
        crate::database::tests::seed_database(&mut database);

        let temp_dir = create_temp_dir("arrow_db_test_import").await;

        // First export the data to create the files
        get_mut_table!(database, "users")
            .unwrap()
            .export_parquet_to_disk(&temp_dir)
            .await
            .unwrap();

        // Clear the table data
        let cleared_table = crate::table::Table::new("users");
        database.tables.insert("users", cleared_table);

        // Now test importing it
        get_mut_table!(database, "users")
            .unwrap()
            .import_parquet_from_disk(&temp_dir)
            .await
            .unwrap();

        get_table!(database, "users").unwrap().print();

        // Clean up
        remove_temp_dir(&temp_dir).await;
    }
}
