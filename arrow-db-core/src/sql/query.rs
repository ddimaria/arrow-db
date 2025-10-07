//! Query SQL operations in DataFusion.
//!
//!

use datafusion::{
    logical_expr::{DmlStatement, LogicalPlan, WriteOp},
    prelude::DataFrame,
};

use crate::{
    database::Database,
    error::{DbError, Result},
    pagination::PaginationInfo,
};

impl<'a> Database<'a> {
    /// Run a SQL query, returning a `DataFrame`
    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

        let logical_plan = df.logical_plan().to_owned();

        // handle DML operations (INSERT, UPDATE, DELETE)
        match &logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op,
                input,
                ..
            }) => {
                let table_name_str = table_name.table();

                match op {
                    WriteOp::Update => {
                        let updated_count = self.execute_update(table_name_str, input).await?;
                        println!("Updated {} rows", updated_count);

                        // context update needed - MemTable copies data at registration time
                        self.add_table_context(table_name_str)?;

                        // return a simple result DataFrame showing the update count
                        let result_df = self
                            .ctx
                            .sql(&format!("SELECT {} as updated_rows", updated_count))
                            .await
                            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;
                        return Ok(result_df);
                    }
                    WriteOp::InsertInto => {
                        let inserted_count = self.execute_insert(table_name_str, input).await?;
                        println!("Inserted {} rows", inserted_count);

                        // context update needed - MemTable copies data at registration time
                        self.add_table_context(table_name_str)?;

                        let result_df = self
                            .ctx
                            .sql(&format!("SELECT {} as count", inserted_count))
                            .await
                            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;
                        return Ok(result_df);
                    }
                    WriteOp::Delete => {
                        let deleted_count = self.execute_delete(table_name_str, input).await?;
                        println!("Deleted {} rows", deleted_count);

                        // context update needed - MemTable copies data at registration time
                        self.add_table_context(table_name_str)?;

                        let result_df = self
                            .ctx
                            .sql(&format!("SELECT {} as deleted_rows", deleted_count))
                            .await
                            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;
                        return Ok(result_df);
                    }
                    _ => {
                        // for other operations, fall through to normal query execution
                    }
                }
            }
            _ => {
                // for non-DML queries, proceed normally
            }
        }

        // for regular SELECT queries and other operations
        Ok(df)
    }

    /// Run a SQL query with pagination
    ///
    /// Uses the DataFrame API for efficient, lazy pagination without SQL string manipulation.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute
    /// * `page` - The page number (0-indexed)
    /// * `page_size` - Number of rows per page
    /// * `include_total_count` - Whether to compute total row count (can be expensive)
    ///
    /// # Returns
    /// A tuple of (DataFrame, PaginationInfo)
    pub async fn query_paginated(
        &self,
        sql: &str,
        page: usize,
        page_size: usize,
        include_total_count: bool,
    ) -> Result<(DataFrame, PaginationInfo)> {
        // Execute the base query to get a DataFrame
        let df = self.query(sql).await?;

        // Get total count if requested (before applying pagination)
        let total_rows = if include_total_count {
            // Clone the dataframe and collect to count total rows
            let count_batches = df
                .clone()
                .collect()
                .await
                .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

            let total: usize = count_batches.iter().map(|b| b.num_rows()).sum();
            Some(total)
        } else {
            None
        };

        // Apply pagination using DataFrame API
        let offset = page * page_size;
        let paginated_df = df
            .limit(offset, Some(page_size))
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

        // Collect to get actual row count in this page
        let batches = paginated_df
            .collect()
            .await
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

        let rows_in_page = batches.iter().map(|b| b.num_rows()).sum();

        // Create pagination info
        let pagination_info = PaginationInfo::new(page, page_size, rows_in_page, total_rows);

        // Recreate DataFrame from batches
        let df = self
            .ctx
            .read_batches(batches)
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

        Ok((df, pagination_info))
    }

    #[cfg(test)]
    pub async fn test_query(&self, sql: &str) {
        println!("\n{}", sql);
        self.query(sql).await.unwrap().show().await.unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{database::Database, get_mut_table, table::Table};
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;

    // Helper function to test queries and expected row counts
    pub async fn test_query(
        database: &Database<'_>,
        query: &str,
        expected_rows: usize,
        description: &str,
    ) {
        let result = database.query(query).await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), expected_rows, "{}", description);
    }

    // Create a test database with more rows for pagination testing
    fn create_test_database_with_data<'a>(num_rows: usize) -> Database<'a> {
        let mut database = Database::new("TestDB").unwrap();

        let table = Table::new("test_table");
        database.add_table(table).unwrap();

        // Generate test data
        let ids: Vec<i32> = (1..=num_rows as i32).collect();
        let names: Vec<String> = (1..=num_rows).map(|i| format!("User{}", i)).collect();
        let ages: Vec<i32> = (1..=num_rows as i32).map(|i| 20 + (i % 50)).collect();

        get_mut_table!(database, "test_table")
            .unwrap()
            .add_column::<Int32Array>(0, "id", DataType::Int32, Int32Array::from(ids).into())
            .unwrap();

        get_mut_table!(database, "test_table")
            .unwrap()
            .add_column::<StringArray>(1, "name", DataType::Utf8, StringArray::from(names).into())
            .unwrap();

        get_mut_table!(database, "test_table")
            .unwrap()
            .add_column::<Int32Array>(2, "age", DataType::Int32, Int32Array::from(ages).into())
            .unwrap();

        database.add_table_context("test_table").unwrap();
        database
    }

    #[tokio::test]
    async fn test_pagination_first_page() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 0, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 10, "Should return 10 rows for first page");
        assert_eq!(pagination.page, 0, "Page should be 0");
        assert_eq!(pagination.page_size, 10, "Page size should be 10");
        assert_eq!(pagination.rows_in_page, 10, "Should have 10 rows in page");
        assert_eq!(pagination.total_rows, Some(50), "Total should be 50");
        assert_eq!(pagination.total_pages, Some(5), "Should have 5 pages");
        assert!(pagination.has_next_page, "Should have next page");
        assert!(
            !pagination.has_previous_page,
            "Should not have previous page"
        );
    }

    #[tokio::test]
    async fn test_pagination_middle_page() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 2, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 10, "Should return 10 rows");
        assert_eq!(pagination.page, 2, "Page should be 2");
        assert!(pagination.has_next_page, "Should have next page");
        assert!(pagination.has_previous_page, "Should have previous page");
    }

    #[tokio::test]
    async fn test_pagination_last_page() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 4, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 10, "Should return 10 rows for last page");
        assert_eq!(pagination.page, 4, "Page should be 4");
        assert!(!pagination.has_next_page, "Should not have next page");
        assert!(pagination.has_previous_page, "Should have previous page");
    }

    #[tokio::test]
    async fn test_pagination_partial_last_page() {
        let database = create_test_database_with_data(25);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 2, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 5, "Should return 5 rows for partial last page");
        assert_eq!(pagination.rows_in_page, 5, "Should have 5 rows in page");
        assert_eq!(pagination.total_pages, Some(3), "Should have 3 pages");
        assert!(!pagination.has_next_page, "Should not have next page");
    }

    #[tokio::test]
    async fn test_pagination_beyond_available_data() {
        let database = create_test_database_with_data(25);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 5, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 0, "Should return 0 rows beyond available data");
        assert_eq!(pagination.rows_in_page, 0, "Should have 0 rows in page");
        assert!(!pagination.has_next_page, "Should not have next page");
    }

    #[tokio::test]
    async fn test_pagination_without_total_count() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 1, 10, false)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 10, "Should return 10 rows");
        assert_eq!(
            pagination.total_rows, None,
            "Total should be None when not requested"
        );
        assert_eq!(pagination.total_pages, None, "Total pages should be None");
        assert!(
            pagination.has_next_page,
            "Should assume next page exists (full page)"
        );
    }

    #[tokio::test]
    async fn test_pagination_different_page_sizes() {
        let database = create_test_database_with_data(100);

        // Test with page size 25
        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 0, 25, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 25, "Should return 25 rows");
        assert_eq!(
            pagination.total_pages,
            Some(4),
            "Should have 4 pages with size 25"
        );

        // Test with page size 100
        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table", 0, 100, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 100, "Should return all 100 rows");
        assert_eq!(
            pagination.total_pages,
            Some(1),
            "Should have 1 page with size 100"
        );
    }

    #[tokio::test]
    async fn test_pagination_with_where_clause() {
        let database = create_test_database_with_data(100);

        // Query with WHERE clause (age > 40 will match some users)
        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table WHERE age > 40", 0, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert!(total_rows <= 10, "Should return at most 10 rows");
        assert!(pagination.total_rows.is_some(), "Should have total count");
        assert!(
            pagination.total_rows.unwrap() < 100,
            "Filtered results should be less than 100"
        );
    }

    #[tokio::test]
    async fn test_pagination_with_order_by() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table ORDER BY id DESC", 0, 5, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();

        // Verify ordering - first row should have highest ID
        let first_batch = &batches[0];
        let id_col = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            id_col.value(0),
            50,
            "First row should have ID 50 (descending order)"
        );

        assert_eq!(pagination.rows_in_page, 5, "Should have 5 rows");
        assert_eq!(pagination.total_rows, Some(50), "Should have 50 total");
    }

    #[tokio::test]
    async fn test_pagination_with_aggregation() {
        let database = create_test_database_with_data(100);

        // GROUP BY query
        let (df, pagination) = database
            .query_paginated(
                "SELECT age, COUNT(*) as count FROM test_table GROUP BY age",
                0,
                5,
                true,
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 5, "Should return 5 grouped rows");
        assert!(pagination.total_rows.is_some(), "Should have total count");
    }

    #[tokio::test]
    async fn test_pagination_empty_result() {
        let database = create_test_database_with_data(50);

        let (df, pagination) = database
            .query_paginated("SELECT * FROM test_table WHERE id > 1000", 0, 10, true)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 0, "Should return 0 rows for empty result");
        assert_eq!(pagination.total_rows, Some(0), "Total should be 0");
        assert_eq!(pagination.rows_in_page, 0, "Rows in page should be 0");
        assert!(!pagination.has_next_page, "Should not have next page");
    }

    #[tokio::test]
    async fn test_pagination_data_consistency() {
        let database = create_test_database_with_data(30);

        // Fetch page 0
        let (df0, _) = database
            .query_paginated("SELECT * FROM test_table ORDER BY id", 0, 10, false)
            .await
            .unwrap();
        let batches0 = df0.collect().await.unwrap();
        let id_col0 = batches0[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Fetch page 1
        let (df1, _) = database
            .query_paginated("SELECT * FROM test_table ORDER BY id", 1, 10, false)
            .await
            .unwrap();
        let batches1 = df1.collect().await.unwrap();
        let id_col1 = batches1[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Verify no overlap and correct ordering
        assert_eq!(id_col0.value(0), 1, "First page should start with ID 1");
        assert_eq!(id_col0.value(9), 10, "First page should end with ID 10");
        assert_eq!(id_col1.value(0), 11, "Second page should start with ID 11");
        assert_eq!(id_col1.value(9), 20, "Second page should end with ID 20");
    }
}
