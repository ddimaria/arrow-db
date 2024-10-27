use std::sync::Arc;

use arrow::{
    array::{Array, ArrayData, ArrayRef, RecordBatch},
    util::pretty,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::TableSource;

use crate::{
    column::Column,
    error::{DbError, Result},
};

#[derive(Debug, Clone, PartialEq)]
pub struct Table<'a> {
    pub name: &'a str,
    pub schema: Schema,
    pub columns: Vec<Column>,
}

impl<'a> Table<'a> {
    pub fn new(name: &'a str) -> Table<'a> {
        Table {
            name,
            schema: Schema::empty(),
            columns: vec![],
        }
    }

    /// Ensure that the column index is within the bounds of the table schema.
    pub fn column_index_in_bounds(&self, column_index: usize) -> Result<()> {
        if column_index > self.schema.fields().len() {
            return Err(DbError::ColumnIndexOutOfBounds(
                column_index,
                self.name.into(),
            ));
        }

        Ok(())
    }

    /// Add a column to the table schema at a given index.
    /// If the index is out of bounds, return an error.
    ///
    /// Since the schema is immutable, we need to create a new schema with the
    /// new field.
    pub fn add_column(
        &mut self,
        column_index: usize,
        name: &'a str,
        data_type: DataType,
    ) -> Result<()> {
        self.column_index_in_bounds(column_index)?;

        let new_field = Field::new(name, data_type.to_owned(), true);
        let mut fields = self.schema.fields().to_vec();
        fields.insert(column_index, Arc::new(new_field));

        self.schema = Schema::new(fields);

        let data = Column::new_empty(data_type);
        self.columns.insert(column_index, data);

        Ok(())
    }

    /// Append ArrayData to a column in the table.
    pub fn append_column_data<T: std::convert::From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        let column = self.columns.get_mut(column_index).unwrap();
        column.append_data::<T>(data)
    }

    /// Insert ArrayData to a column in the table at a specified row index.
    pub fn insert_column_data_at<T: std::convert::From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        row_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        if data.len() == 0 {
            return Ok(());
        }

        self.column_index_in_bounds(column_index)?;

        let column = self.columns.get_mut(column_index).unwrap();
        column.insert_data_at::<T>(row_index, data)
    }

    pub fn replace_column_data(&mut self, column_index: usize, data: ArrayRef) -> Result<()> {
        self.column_index_in_bounds(column_index)?;

        let column = self.columns.get_mut(column_index).unwrap();
        column.replace_data(data)
    }

    pub fn print(&self) {
        let columns = self
            .columns
            .iter()
            .map(|c| c.data.to_owned())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(self.schema.to_owned()), columns).unwrap();

        pretty::print_batches(&[batch]).unwrap();
    }
}

impl Into<RecordBatch> for Table<'_> {
    fn into(self) -> RecordBatch {
        let columns = self
            .columns
            .iter()
            .map(|c| c.data.to_owned())
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::new(self.schema), columns).unwrap()
    }
}

impl TableSource for Table<'static> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.schema.to_owned())
    }
}

#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_table() {
        let mut table = Table::new("users");

        table.add_column(0, "id", DataType::Int32).unwrap();

        table
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![1, 2]).into())
            .unwrap();

        table
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![3]).into())
            .unwrap();

        table
            .insert_column_data_at::<Int32Array>(0, 2, Int32Array::from(vec![4]).into())
            .unwrap();

        table.add_column(1, "name", DataType::Utf8).unwrap();

        table
            .append_column_data::<StringArray>(
                1,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        table.print();
    }
}
