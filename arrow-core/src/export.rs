use parquet::arrow::AsyncArrowWriter;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::error::{DbError, Result};
use crate::table::Table;

impl<'a> Table<'a> {
    /// Helper function to create a `DbError` for table export errors
    fn export_error(&self, error: impl ToString) -> DbError {
        DbError::TableExportError(self.name.into(), error.to_string())
    }

    /// Generic export the table to a parquet file
    pub async fn export_parquet(&mut self, mut file: File) -> Result<()> {
        let mut buffer = Vec::new();
        let record_batch = &self.record_batch;
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), None)
            .map_err(|e| self.export_error(e))?;

        writer
            .write(&record_batch)
            .await
            .map_err(|e| self.export_error(e))?;
        writer.close().await.map_err(|e| self.export_error(e))?;

        let mut pos = 0;

        while pos < buffer.len() {
            let bytes_written = file
                .write(&buffer[pos..])
                .await
                .map_err(|e| self.export_error(e))?;
            pos += bytes_written;
        }

        Ok(())
    }

    /// Export the table to a parquet file on disk
    pub async fn export_parquet_to_disk(&mut self, path: &str) -> Result<()> {
        let file_name = format!("{path}/{}.parquet", self.name);
        let file = File::create(&file_name)
            .await
            .map_err(|e| self.export_error(e))?;

        self.export_parquet(file).await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        database::tests::{create_database, seed_database},
        get_mut_table,
    };

    #[tokio::test]
    async fn test_export_parquet_to_disk() {
        let (mut database, _) = create_database();
        seed_database(&mut database);

        get_mut_table!(database, "users")
            .unwrap()
            .export_parquet_to_disk(database.name)
            .await
            .unwrap();
    }
}
