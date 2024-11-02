use parquet::arrow::AsyncArrowWriter;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::table::Table;

impl<'a> Table<'a> {
    pub async fn export_parquet(&mut self, mut file: File) -> Result<()> {
        let mut buffer = Vec::new();
        let record_batch = &self.record_batch;
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), None).unwrap();

        writer.write(&record_batch).await.unwrap();
        writer.close().await.unwrap();

        let mut pos = 0;

        while pos < buffer.len() {
            let bytes_written = file.write(&buffer[pos..]).await.unwrap();
            pos += bytes_written;
        }

        Ok(())
    }

    pub async fn export_parquet_to_disk(&mut self, path: &str) -> Result<()> {
        let file_name = format!("{path}/{}.parquet", self.name);
        let file = File::create(&file_name).await.unwrap();

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
