use std::sync::Arc;

use arrow::{
    array::{new_empty_array, Array, ArrayData, ArrayDataBuilder, ArrayRef, RecordBatch},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
    util::pretty,
};
use arrow_schema::{Field, Schema};

use crate::error::{DbError, Result};

#[derive(Debug, Clone)]
pub struct Column {
    pub data: ArrayRef,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

impl Column {
    pub fn new_empty(data_type: DataType) -> Column {
        Column {
            data: new_empty_array(&data_type).into(),
        }
    }

    fn primitive_width(&self) -> Result<usize> {
        self.data.data_type().primitive_width().ok_or_else(|| {
            DbError::DataType(format!(
                "Data type {:?} does not have a primitive width",
                self.data.data_type(),
            ))
        })
    }

    /// Append ArrayData to a column in the table.
    pub fn append_data<T: std::convert::From<ArrayData> + Array + 'static>(
        &mut self,
        data: ArrayData,
    ) -> Result<()> {
        let end = self.data.len();
        self.insert_data_at::<T>(end, data)
    }

    /// Insert ArrayData to a column in the table at a specified row index.
    pub fn insert_data_at<T: std::convert::From<ArrayData> + Array + 'static>(
        &mut self,
        row_index: usize,
        data: ArrayData,
    ) -> Result<()> {
        if data.len() == 0 {
            return Ok(());
        }

        let column = self.data.to_data();
        let column_len: usize = column.len();
        let new_len = column_len + data.len();

        // ignore the empty single buffer of a newly created column
        let buffers = if column_len == 0 {
            data.buffers().to_vec()
        } else {
            let column_buffer = &column.buffers()[0];
            let data_buffer = &data.buffers()[0];
            let mut buffer = MutableBuffer::new(new_len);

            let adjusted_index = row_index * self.primitive_width()?;
            let spliced = column_buffer.split_at(adjusted_index);

            buffer.extend_from_slice(spliced.0);
            buffer.extend_from_slice(data_buffer.as_slice());
            buffer.extend_from_slice(spliced.1);

            vec![Buffer::from(buffer)]
        };

        let array_data = ArrayDataBuilder::from(column)
            .len(new_len)
            .buffers(buffers)
            .build()
            .map_err(|e| DbError::ArrayData(format!("Error building data: {e}")))?;

        self.replace_data(Arc::<T>::new(array_data.into()))?;

        Ok(())
    }

    pub fn replace_data(&mut self, data: ArrayRef) -> Result<()> {
        self.data = data;

        Ok(())
    }

    pub fn print(&self) {
        let schema = Schema::new(vec![Field::new(
            self.data.data_type().to_string(),
            self.data.data_type().to_owned(),
            false,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![self.data.to_owned()]).unwrap();
        pretty::print_batches(&[batch]).unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, StringArray};

    use super::*;

    #[test]
    fn test_int32_column() {
        let mut column = Column::new_empty(DataType::Int32);

        column
            .append_data::<Int32Array>(Int32Array::from(vec![1, 2]).into())
            .unwrap();

        column
            .append_data::<Int32Array>(Int32Array::from(vec![3]).into())
            .unwrap();

        column
            .insert_data_at::<Int32Array>(2, Int32Array::from(vec![4]).into())
            .unwrap();

        column.print();
    }

    #[test]
    fn test_string_column() {
        let mut column = Column::new_empty(DataType::Utf8);

        column
            .append_data::<StringArray>(
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        column.print();
    }
}
