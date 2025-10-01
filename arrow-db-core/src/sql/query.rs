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

    #[cfg(test)]
    pub async fn test_query(&self, sql: &str) {
        println!("\n{}", sql);
        self.query(sql).await.unwrap().show().await.unwrap();
    }
}

#[cfg(test)]
pub mod tests {

    use crate::database::Database;

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
}
