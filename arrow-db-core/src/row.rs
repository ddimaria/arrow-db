//! Row operations
//!
//!

use std::convert::From;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, StringArray,
};

use crate::{
    error::{DbError, Result},
    table::Table,
};

impl<'a> Table<'a> {
    /// Append a complete row to the table by appending to all columns simultaneously
    pub fn append_row(&mut self, row_data: Vec<ArrayRef>) -> Result<()> {
        if row_data.len() != self.record_batch.num_columns() {
            return Err(DbError::CreateRecordBatch(format!(
                "Row data length {} does not match table column count {}",
                row_data.len(),
                self.record_batch.num_columns()
            )));
        }

        let mut new_columns = Vec::new();
        let existing_columns = self.record_batch.columns();

        for (col_idx, new_data) in row_data.iter().enumerate() {
            let existing_column = &existing_columns[col_idx];

            // Concatenate the existing column with the new data
            let concat_result =
                arrow::compute::concat(&[existing_column.as_ref(), new_data.as_ref()]).map_err(
                    |e| DbError::CreateRecordBatch(format!("Error concatenating columns: {}", e)),
                )?;

            new_columns.push(concat_result);
        }

        let schema = self.record_batch.schema();
        self.record_batch = Self::new_record_batch(schema, new_columns)?;

        Ok(())
    }

    /// Delete a single row from a table by reconstructing all columns without that row
    pub(crate) fn delete_row(&mut self, row_idx: usize) -> Result<()> {
        let num_rows = self.record_batch.num_rows();
        let num_columns = self.record_batch.num_columns();

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
            let column = self.record_batch.column(col_idx);
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
        let schema = self.record_batch.schema();
        self.record_batch = Table::new_record_batch(schema, new_columns)?;

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_append_row() {
        let mut table = Table::new("users");

        // First add columns with initial data
        table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3, 4]).into(),
            )
            .unwrap();

        table
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        println!("Before append_row:");
        table.print();

        // Now append a row
        let row_data = vec![
            Arc::new(Int32Array::from(vec![5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Eve"])) as ArrayRef,
        ];

        table.append_row(row_data).unwrap();

        println!("After append_row:");
        table.print();

        // Verify the data
        assert_eq!(table.record_batch.num_rows(), 5);
        let id_column = table.record_batch.column(0);
        let name_column = table.record_batch.column(1);

        let id_array = id_column.as_any().downcast_ref::<Int32Array>().unwrap();
        let name_array = name_column.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(id_array.value(4), 5);
        assert_eq!(name_array.value(4), "Eve");
    }
}
