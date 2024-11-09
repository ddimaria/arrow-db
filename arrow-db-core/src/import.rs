//! Import operations.
//!
//! Tables can be imported from parquet files on disk.

use arrow::compute::concat_batches;
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

use crate::error::{DbError, Result};
use crate::table::Table;

impl<'a> Table<'a> {
    /// Helper function to create a `DbError` for table import errors
    fn import_error(&self, error: impl ToString) -> DbError {
        DbError::TableImportError(self.name.into(), error.to_string())
    }

    /// Generic import the table from a parquet file
    pub async fn import_parquet(&mut self, file: File) -> Result<()> {
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
    pub async fn import_parquet_from_disk(&mut self, path: &str) -> Result<()> {
        let file_name = format!("{path}/{}.parquet", self.name);
        let file = File::open(&file_name)
            .await
            .map_err(|e| self.import_error(e))?;

        self.import_parquet(file).await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{database::tests::create_database, get_mut_table, get_table};

    #[tokio::test]
    async fn test_import_parquet_from_disk() {
        let (database, _) = create_database();

        get_mut_table!(database, "users")
            .unwrap()
            .import_parquet_from_disk(database.name)
            .await
            .unwrap();

        get_table!(database, "users").unwrap().print();
    }
}
