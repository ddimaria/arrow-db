#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;

    use crate::{database::tests::create_database, get_mut_table};

    // use super::*;

    #[tokio::test]
    async fn test_sql() {
        let (mut database, table) = create_database();
        let name = table.name;

        get_mut_table!(database, name)
            .unwrap()
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3, 4]).into(),
            )
            .unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        database.print();

        let table = database.tables.get("users").unwrap().to_owned();

        database.add_table_context(table).unwrap();

        let sql_df = database
            .ctx
            .sql("insert into users values (5, 'Eve')")
            .await
            .unwrap();
        sql_df.show().await.unwrap();

        let sql_df = database
            .ctx
            .sql("select * from users where id > 1 order by name desc")
            .await
            .unwrap();
        sql_df.show().await.unwrap();

        // let sql_df = database
        //     .ctx
        //     .sql("update users set name = 'Eve2' where id = 5")
        //     .await
        //     .unwrap();
        // sql_df.show().await.unwrap();

        // let batch = database.remove_table_context(table).unwrap();
    }
}
