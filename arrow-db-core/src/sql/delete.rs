//! Delete SQL operations in DataFusion.
//!
//!

use std::sync::Arc;

use datafusion::logical_expr::{Expr, LogicalPlan};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, StringArray,
};

use crate::{
    database::Database,
    error::{DbError, Result},
    get_mut_table, get_table,
    table::Table,
};

/// Represents parsed DELETE components
#[derive(Debug, Clone)]
pub struct DeleteComponents {
    pub where_condition: Option<Expr>,
}

impl<'a> Database<'a> {
    /// Parse DELETE logical plan to extract WHERE clause
    pub(crate) fn parse_delete_plan(&self, input: &LogicalPlan) -> Result<DeleteComponents> {
        // for DELETE, we mainly need to extract the WHERE condition
        let where_condition = self.extract_where_condition(input)?;

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
            self.delete_row_from_table(table_name, row_idx)?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }

    /// Delete a single row from a table by reconstructing all columns without that row
    pub(crate) fn delete_row_from_table(&self, table_name: &str, row_idx: usize) -> Result<()> {
        let mut table = get_mut_table!(self, table_name)?;
        let num_rows = table.record_batch.num_rows();
        let num_columns = table.record_batch.num_columns();

        if row_idx >= num_rows {
            return Err(DbError::Query(
                format!(
                    "Row index {} out of bounds (table has {} rows)",
                    row_idx, num_rows
                ),
                "".into(),
            ));
        }

        // create new columns without the specified row
        let mut new_columns = Vec::new();

        for col_idx in 0..num_columns {
            let column = table.record_batch.column(col_idx);
            let data_type = column.data_type();

            match data_type {
                arrow::datatypes::DataType::Int32 => {
                    if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if int_array.is_null(i) {
                                    None
                                } else {
                                    Some(int_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(Int32Array::from(values)) as ArrayRef);
                    }
                }
                arrow::datatypes::DataType::Float32 => {
                    if let Some(float_array) = column.as_any().downcast_ref::<Float32Array>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if float_array.is_null(i) {
                                    None
                                } else {
                                    Some(float_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(Float32Array::from(values)) as ArrayRef);
                    }
                }
                arrow::datatypes::DataType::Float64 => {
                    if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if float_array.is_null(i) {
                                    None
                                } else {
                                    Some(float_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                    }
                }
                arrow::datatypes::DataType::Boolean => {
                    if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if bool_array.is_null(i) {
                                    None
                                } else {
                                    Some(bool_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
                    }
                }
                arrow::datatypes::DataType::Date32 => {
                    if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if date_array.is_null(i) {
                                    None
                                } else {
                                    Some(date_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(Date32Array::from(values)) as ArrayRef);
                    }
                }
                arrow::datatypes::DataType::Utf8 => {
                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                        let mut values = Vec::new();
                        for i in 0..num_rows {
                            if i != row_idx {
                                values.push(if string_array.is_null(i) {
                                    None
                                } else {
                                    Some(string_array.value(i))
                                });
                            }
                        }
                        new_columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }
                }
                _ => {
                    return Err(DbError::Query(
                        format!("Unsupported data type for DELETE: {:?}", data_type),
                        "".into(),
                    ));
                }
            }
        }

        // replace the entire record batch with the new columns
        let schema = table.record_batch.schema();
        table.record_batch = Table::new_record_batch(schema, new_columns)?;

        Ok(())
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
