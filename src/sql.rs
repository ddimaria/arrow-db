#[cfg(test)]
pub mod tests {
    use arrow::array::Int32Array;
    use arrow_schema::DataType;

    use crate::{database::tests::create_database, get_mut_table};

    // use super::*;

    #[tokio::test]
    async fn test_sql() {
        let (mut database, table) = create_database();
        let name = table.name;

        get_mut_table!(database, name)
            .unwrap()
            .add_column(0, "id", DataType::Int32)
            .unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![1, 2]).into())
            .unwrap();

        database.print();

        let table = database.tables.get("users").unwrap().to_owned();

        database.add_table_context(table).unwrap();

        let sql_df = database
            .ctx
            .sql("select * from users where id = 1")
            .await
            .unwrap();
        sql_df.show().await.unwrap();
    }
}
