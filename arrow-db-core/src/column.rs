//! Column operations
//!
//! Columns are a single data type and must be of equal length to other columns
//! in a table.
//!
//! Arrow data structures are immutable, so they need to be manipulated and
//! recreated for any changes in the structure.

use std::convert::From;
use std::sync::Arc;

use arrow::{
    array::{Array, ArrayData, ArrayDataBuilder, ArrayRef, RecordBatch, StringArray},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
};
use arrow_schema::{Field, Schema};

use crate::{
    error::{DbError, Result},
    table::Table,
};

/// The kind of set operation to perform on a column.
pub enum SetKind {
    Append(ArrayData),
    InsertAt(ArrayData),
    Update(ArrayData),
    Remove,
}

impl SetKind {
    pub fn get_data(&self) -> Option<&ArrayData> {
        match self {
            SetKind::Append(data) => Some(data),
            SetKind::InsertAt(data) => Some(data),
            SetKind::Update(data) => Some(data),
            SetKind::Remove => None,
        }
    }
}

impl<'a> Table<'a> {
    /// Get the primitive width of a data type
    fn column_primitive_width(&self, data: &DataType) -> Result<usize> {
        data.primitive_width().ok_or_else(|| {
            DbError::DataType(format!(
                "Data type {:?} does not have a primitive width",
                data,
            ))
        })
    }

    /// Ensure that the column index is within the bounds of the table schema.
    pub fn column_index_in_bounds(&self, column_index: usize) -> Result<()> {
        if column_index > self.record_batch.schema_ref().fields().len() {
            return Err(DbError::ColumnIndexOutOfBounds(
                column_index,
                self.name.into(),
            ));
        }

        Ok(())
    }

    /// Create a new `RecordBatch` from a schema and columns
    pub fn new_record_batch(
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn Array>>,
    ) -> Result<RecordBatch> {
        RecordBatch::try_new(schema, columns)
            .map_err(|e| DbError::CreateRecordBatch(format!("Error creating RecordBatch: {e}")))
    }

    /// Add a column to the table schema at a given index.
    /// If the index is out of bounds, return an error.
    ///
    /// Since the schema is immutable, we need to create a new schema with the
    /// new field.
    pub fn add_column<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        name: &'a str,
        data_type: DataType,
        data: ArrayData,
    ) -> Result<()> {
        self.column_index_in_bounds(column_index)?;

        let new_field = Field::new(name, data_type.to_owned(), true);
        let mut fields = self.record_batch.schema().fields().to_vec();
        fields.insert(column_index, Arc::new(new_field));

        let mut columns = self.record_batch.columns().to_vec();
        let column: ArrayRef = Arc::<T>::new(data.into());
        columns.push(Arc::new(column));

        let schema = Arc::new(Schema::new(fields));
        self.record_batch = Self::new_record_batch(schema, columns)?;

        Ok(())
    }

    /// Append ArrayData to a column in the table.
    pub fn append_column_data<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        let column = self.record_batch.column(column_index);
        let end = column.to_data().len();

        self.insert_column_data::<T>(column_index, end, data)
    }

    /// Insert ArrayData to a column in the table at a specified row index.
    pub fn insert_column_data<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        row_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        let set_kind = SetKind::InsertAt(data);
        self.set_column_data::<T>(column_index, row_index, set_kind)
    }

    /// Update ArrayData to a column in the table at a specified row index.
    pub fn update_column_data<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        row_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        // For variable-length types like strings, use a reconstruction approach
        let column = self.record_batch.column(column_index);
        let data_type = column.data_type();

        match data_type {
            DataType::Utf8 => {
                // For UTF-8 strings, reconstruct the entire column
                self.update_string_column_data(column_index, row_index, data)
            }
            _ => {
                // For primitive types, use the existing buffer manipulation approach
                let set_kind = SetKind::Update(data);
                self.set_column_data::<T>(column_index, row_index, set_kind)
            }
        }
    }

    /// Update a string column at a specific row index by reconstructing the column
    fn update_string_column_data(
        &mut self,
        column_index: usize,
        row_index: usize,
        new_data: ArrayData,
    ) -> Result<()> {
        let existing_column = self.record_batch.column(column_index);
        let existing_array = existing_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DbError::DataType("Expected StringArray".into()))?;

        let new_array = StringArray::from(new_data);
        if new_array.len() != 1 {
            return Err(DbError::DataType(
                "Update data must contain exactly one element".into(),
            ));
        }

        let new_value = new_array.value(0);

        // Reconstruct the entire column with the updated value
        let mut values = Vec::new();
        for i in 0..existing_array.len() {
            if i == row_index {
                values.push(new_value);
            } else {
                values.push(existing_array.value(i));
            }
        }

        let updated_array = StringArray::from(values);
        self.replace_column_data(column_index, Arc::new(updated_array))?;

        Ok(())
    }

    /// Remove a column in the table at a specified row index.
    pub fn remove_column_data<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        row_index: usize,
    ) -> Result<()> {
        let set_kind = SetKind::Remove;
        self.set_column_data::<T>(column_index, row_index, set_kind)
    }

    /// Set ArrayData column in the table at a specified row index.
    pub fn set_column_data<T: From<ArrayData> + Array + 'static>(
        &mut self,
        column_index: usize,
        row_index: usize,
        set_kind: SetKind,
    ) -> Result<()> {
        let data = set_kind.get_data();

        let column = self.record_batch.column(column_index);
        let column_data = column.to_data();
        let column_len: usize = column.len();
        let new_len = match set_kind {
            SetKind::Append(ref data) => column_len + data.len(),
            SetKind::InsertAt(ref data) => column_len + data.len(),
            SetKind::Update(_) => column_len,
            SetKind::Remove => column_len - 1,
        };

        // ignore the empty single buffer of a newly created column
        let buffers = if column_len == 0 {
            data.map_or_else(std::vec::Vec::new, |data| data.buffers().to_vec())
        } else {
            let column_buffer = &column_data.buffers()[0];
            let mut buffer = MutableBuffer::new(new_len);

            let width = self.column_primitive_width(column.data_type())?;
            let adjusted_index = row_index * width;
            let spliced = column_buffer.split_at(adjusted_index);

            let end = match set_kind {
                SetKind::Append(_) => spliced.1,
                SetKind::InsertAt(_) => spliced.1,
                SetKind::Update(_) => &spliced.1[width..],
                SetKind::Remove => &spliced.1[width..],
            };

            buffer.extend_from_slice(spliced.0);

            if let Some(data) = data {
                let data_buffer = &data.buffers()[0];
                buffer.extend_from_slice(data_buffer.as_slice());
            }

            buffer.extend_from_slice(end);

            vec![Buffer::from(buffer)]
        };

        let array_data = ArrayDataBuilder::from(column_data)
            .len(new_len)
            .buffers(buffers)
            .build()
            .map_err(|e| DbError::ArrayData(format!("Error building data: {e}")))?;

        self.replace_column_data(column_index, Arc::<T>::new(array_data.into()))?;

        Ok(())
    }

    /// Replace a column in the table with a new `ArrayRef`
    pub fn replace_column_data(&mut self, column_index: usize, data: ArrayRef) -> Result<()> {
        let mut columns = self.record_batch.columns().to_vec();
        columns[column_index] = data;

        let schema = self.record_batch.schema();
        self.record_batch = Self::new_record_batch(schema, columns)?;

        Ok(())
    }

    #[cfg(test)]
    pub fn print_column(&self, column_index: usize) {
        let column = self.record_batch.column(column_index).to_owned();
        let schema = Schema::new(vec![Field::new(
            column.data_type().to_string(),
            column.data_type().to_owned(),
            false,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![column]).unwrap();
        arrow::util::pretty::print_batches(&[batch]).unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, StringArray, UnionArray};
    use arrow_schema::{UnionFields, UnionMode};

    use super::*;

    #[test]
    fn test_int32_column() {
        let mut table = Table::new("users");

        // create the column and seed it with data
        table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2]).into(),
            )
            .unwrap();

        // append data to the column
        table
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![3]).into())
            .unwrap();

        // insert data at a specific index in the column
        table
            .insert_column_data::<Int32Array>(0, 2, Int32Array::from(vec![4]).into())
            .unwrap();

        // update data at a specific index in the column
        table
            .update_column_data::<Int32Array>(0, 1, Int32Array::from(vec![5]).into())
            .unwrap();

        table.print_column(0);

        let expected = Int32Array::from(vec![1, 5, 4, 3]).to_data();
        let data = table.record_batch.column(0).to_data();
        assert_eq!(expected, data);

        // remove data at a specific index in the column
        table.remove_column_data::<Int32Array>(0, 1).unwrap();
        let expected = Int32Array::from(vec![1, 4, 3]).to_data();
        let data = table.record_batch.column(0).to_data();
        assert_eq!(expected, data);
    }

    #[test]
    fn test_string_column() {
        let mut table = Table::new("users");
        table
            .add_column::<StringArray>(
                0,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        table.print_column(0);

        let expected = StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).to_data();
        let data = table.record_batch.column(0).to_data();
        assert_eq!(expected, data);
    }

    #[test]
    fn test_union_column() {
        let mut table = Table::new("users");
        let fields = UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("field0", DataType::Int32, false),
                Field::new("field1", DataType::Utf8, false),
            ],
        );

        let int_array = Int32Array::from(vec![1, 2, 3, 4]);
        let string_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]);

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(string_array),
        ];

        let array = UnionArray::try_new(
            fields.clone(),
            vec![0, 1, 0, 1, 0, 1, 0, 1].into(),
            Some(vec![0, 0, 1, 1, 2, 2, 3, 3].into()),
            children,
        )
        .unwrap();

        table
            .add_column::<UnionArray>(
                0,
                "name",
                DataType::Union(fields, UnionMode::Dense),
                array.into(),
            )
            .unwrap();

        table.print();

        let data = table.record_batch.column(0).to_data();
        println!("{:?}", data);
    }
}
