//! Delete SQL operations in DataFusion.
//!
//!

use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::{database::Database, error::Result, get_mut_table, get_table};

/// Represents parsed DELETE components
#[derive(Debug, Clone)]
pub struct DeleteComponents {
    pub where_condition: Option<Expr>,
}

impl<'a> Database<'a> {
    /// Parse DELETE logical plan to extract WHERE clause
    pub(crate) fn parse_delete_plan(&self, input: &LogicalPlan) -> Result<DeleteComponents> {
        // for DELETE, we mainly need to extract the WHERE condition
        let where_condition = Self::extract_where_condition(input)?;

        Ok(DeleteComponents { where_condition })
    }

    /// Execute DELETE operation on the table data
    pub(crate) async fn execute_delete(
        &self,
        table_name: &str,
        input: &LogicalPlan,
    ) -> Result<usize> {
        // parse the logical plan to extract WHERE conditions
        let delete_components = self.parse_delete_plan(input)?;

        let num_rows = {
            let table = get_table!(self, table_name)?;
            table.record_batch.num_rows()
        };

        // find rows that match the WHERE condition
        let rows_to_delete: Vec<usize> = {
            let mut matching_rows = Vec::new();

            for row_idx in 0..num_rows {
                let should_delete = if let Some(ref condition) = delete_components.where_condition {
                    self.evaluate_where_condition(condition, table_name, row_idx)?
                } else {
                    // if no WHERE clause, delete all rows
                    true
                };

                if should_delete {
                    matching_rows.push(row_idx);
                }
            }

            matching_rows
        };

        // delete rows in reverse order to maintain correct indices
        let mut deleted_count = 0;
        for &row_idx in rows_to_delete.iter().rev() {
            let mut table = get_mut_table!(self, table_name)?;
            table.delete_row(row_idx)?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }
}

#[cfg(test)]
pub mod tests {

    use crate::database::tests::{create_database, seed_database};

    #[tokio::test]
    async fn test_delete_with_where() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // First insert a row to delete
        database
            .query("insert into users values (5, 'Eve')")
            .await
            .unwrap();

        // Test DELETE with WHERE clause
        let result = database
            .query("delete from users where id = 5")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();

        // Should return a delete count result
        assert!(!batches.is_empty(), "DELETE should return a result");
    }

    #[tokio::test]
    async fn test_delete_without_where() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test DELETE without WHERE clause (deletes all rows)
        let result = database.query("delete from user_role").await.unwrap();
        let batches = result.collect().await.unwrap();

        // Should return a delete count result
        assert!(!batches.is_empty(), "DELETE should return a result");
    }
}
