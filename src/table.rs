use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::logical_expr::TableSource;

#[derive(Debug, Clone, PartialEq)]
pub struct Table<'a> {
    pub name: &'a str,
    pub record_batch: RecordBatch,
}

impl<'a> Table<'a> {
    pub fn new(name: &'a str) -> Table<'a> {
        let schema = Arc::new(Schema::empty());

        Table {
            name,
            record_batch: RecordBatch::new_empty(schema),
        }
    }

    #[cfg(test)]
    pub fn print(&self) {
        arrow::util::pretty::print_batches(&[self.record_batch.to_owned()]).unwrap();
    }
}

// impl Into<RecordBatch> for Table<'_> {
//     fn into(self) -> RecordBatch {
//         let columns = self
//             .columns
//             .iter()
//             .map(|c| c.data.to_owned())
//             .collect::<Vec<_>>();

//         RecordBatch::try_new(Arc::new(self.schema), columns).unwrap()
//     }
// }

impl TableSource for Table<'static> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.record_batch.schema()
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

        table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2]).into(),
            )
            .unwrap();

        table
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![3]).into())
            .unwrap();

        table
            .insert_column_data_at::<Int32Array>(0, 2, Int32Array::from(vec![4]).into())
            .unwrap();

        table
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        table.print();
    }
}
