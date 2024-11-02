use arrow::compute::concat_batches;
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

use crate::error::Result;
use crate::table::Table;

impl<'a> Table<'a> {
    pub async fn import_parquet(&mut self, file: File) -> Result<()> {
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(8192);

        let stream = builder.build().unwrap();
        let record_batches = stream.try_collect::<Vec<_>>().await.unwrap();

        if let Some(batch) = record_batches.first() {
            let schema = batch.schema();
            self.record_batch = concat_batches(&schema, &record_batches).unwrap();
        }

        Ok(())
    }

    pub async fn import_parquet_from_disk(&mut self, path: &str) -> Result<()> {
        let file_name = format!("{path}/{}.parquet", self.name);
        let file = File::open(&file_name).await.unwrap();

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
