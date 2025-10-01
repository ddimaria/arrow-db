//! Update SQL operations in DataFusion.
//!
//!

use datafusion::{
    logical_expr::{Expr, LogicalPlan},
    scalar::ScalarValue,
};

use arrow::array::{
    BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, StringArray,
};

use crate::{
    database::Database,
    error::{DbError, Result},
    get_mut_table, get_table,
};

/// Represents an UPDATE SET assignment
#[derive(Debug, Clone)]
pub struct SetAssignment {
    pub column: String,
    pub value: ScalarValue,
}

/// Represents parsed UPDATE components
#[derive(Debug, Clone)]
pub struct UpdateComponents {
    pub set_assignments: Vec<SetAssignment>,
    pub where_condition: Option<Expr>,
}

impl<'a> Database<'a> {
    /// Parse UPDATE logical plan to extract SET and WHERE clauses
    pub(crate) fn parse_update_plan(&self, input: &LogicalPlan) -> Result<UpdateComponents> {
        match input {
            LogicalPlan::Projection(projection) => {
                // the UPDATE input is typically a projection that contains the SET assignments
                let mut set_assignments = Vec::new();

                // extract SET assignments from the projection expressions
                for expr in &projection.expr {
                    // Look for Alias expressions that wrap literals (SET assignments)
                    if let Expr::Alias(alias) = expr {
                        let column_name = alias.name.clone();

                        // check if the aliased expression is a literal (SET assignment)
                        if let Some(value) = self.extract_scalar_value(&alias.expr) {
                            set_assignments.push(SetAssignment {
                                column: column_name,
                                value,
                            });
                        }
                    }
                }

                // try to extract WHERE condition from the input of the projection
                let where_condition = self.extract_where_condition(&projection.input)?;

                Ok(UpdateComponents {
                    set_assignments,
                    where_condition,
                })
            }
            _ => {
                // for now, return empty components if we can't parse the plan
                Ok(UpdateComponents {
                    set_assignments: Vec::new(),
                    where_condition: None,
                })
            }
        }
    }

    /// Execute UPDATE operation on the table data
    pub(crate) async fn execute_update(
        &self,
        table_name: &str,
        input: &LogicalPlan,
    ) -> Result<usize> {
        // parse the logical plan to extract SET assignments and WHERE conditions
        let update_components = self.parse_update_plan(input)?;

        if update_components.set_assignments.is_empty() {
            return Err(DbError::Query(
                "No SET assignments found in UPDATE".into(),
                "".into(),
            ));
        }

        let mut updated_count = 0;
        let num_rows = {
            let table = get_table!(self, table_name)?;
            table.record_batch.num_rows()
        };

        // find rows that match the WHERE condition
        let rows_to_update: Vec<usize> = {
            let mut matching_rows = Vec::new();

            for row_idx in 0..num_rows {
                let should_update = if let Some(ref condition) = update_components.where_condition {
                    self.evaluate_where_condition(condition, table_name, row_idx)?
                } else {
                    // if no WHERE clause, update all rows
                    true
                };

                if should_update {
                    matching_rows.push(row_idx);
                }
            }

            matching_rows
        };

        // apply SET assignments to matching rows
        for row_idx in rows_to_update {
            for assignment in &update_components.set_assignments {
                self.apply_set_assignment(table_name, row_idx, assignment)?;
            }
            updated_count += 1;
        }

        Ok(updated_count)
    }

    /// Apply a single SET assignment to a row
    pub(crate) fn apply_set_assignment(
        &self,
        table_name: &str,
        row_idx: usize,
        assignment: &SetAssignment,
    ) -> Result<()> {
        let column_index = {
            let table = get_table!(self, table_name)?;
            let schema = table.record_batch.schema();
            schema
                .column_with_name(&assignment.column)
                .map(|(idx, _)| idx)
                .ok_or_else(|| {
                    DbError::Query(
                        format!("Column '{}' not found", assignment.column),
                        "".into(),
                    )
                })?
        };

        let mut table_mut = get_mut_table!(self, table_name)?;

        match &assignment.value {
            ScalarValue::Int32(Some(value)) => {
                table_mut.update_column_data::<Int32Array>(
                    column_index,
                    row_idx,
                    Int32Array::from(vec![*value]).into(),
                )?;
            }
            ScalarValue::Float32(Some(value)) => {
                table_mut.update_column_data::<Float32Array>(
                    column_index,
                    row_idx,
                    Float32Array::from(vec![*value]).into(),
                )?;
            }
            ScalarValue::Float64(Some(value)) => {
                table_mut.update_column_data::<Float64Array>(
                    column_index,
                    row_idx,
                    Float64Array::from(vec![*value]).into(),
                )?;
            }
            ScalarValue::Boolean(Some(value)) => {
                table_mut.update_column_data::<BooleanArray>(
                    column_index,
                    row_idx,
                    BooleanArray::from(vec![*value]).into(),
                )?;
            }
            ScalarValue::Date32(Some(value)) => {
                table_mut.update_column_data::<Date32Array>(
                    column_index,
                    row_idx,
                    Date32Array::from(vec![*value]).into(),
                )?;
            }
            ScalarValue::Utf8(Some(value)) => {
                table_mut.update_column_data::<StringArray>(
                    column_index,
                    row_idx,
                    StringArray::from(vec![value.clone()]).into(),
                )?;
            }
            _ => {
                return Err(DbError::Query(
                    format!("Unsupported data type for column '{}'", assignment.column),
                    "".into(),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use crate::database::tests::{create_database, seed_database};

    #[tokio::test]
    async fn test_update_with_where() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // First insert a row to update
        database
            .query("insert into users values (5, 'Eve')")
            .await
            .unwrap();

        // Test UPDATE with WHERE clause
        let result = database
            .query("update users set name = 'Eve2' where id = 5")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();

        // Should return an update count result
        assert!(!batches.is_empty(), "UPDATE should return a result");
    }

    #[tokio::test]
    async fn test_update_without_where() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test UPDATE without WHERE clause (updates all rows)
        let result = database
            .query("update user_role set role = 'updated'")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();

        // Should return an update count result
        assert!(!batches.is_empty(), "UPDATE should return a result");
    }
}
