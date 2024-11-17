use std::collections::HashSet;

use arrow::{
    array::{
        Array, ArrayAccessor, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    },
    datatypes::{DataType, Date32Type, Date64Type},
};
use serde::{Deserialize, Serialize};

use crate::log;

#[derive(Serialize, Deserialize)]
pub struct SerializableRecordBatch {
    data: Vec<Vec<Option<String>>>,
}

pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

pub fn to_serializable(
    headers: &Vec<String>,
    record_batch: &RecordBatch,
) -> SerializableRecordBatch {
    let headers = headers
        .iter()
        .map(|header| Some(header.clone()))
        .collect::<Vec<_>>();

    let mut data = vec![headers];
    let mut unsupported = HashSet::new();

    for row in 0..record_batch.num_rows() {
        let mut row_data = Vec::new();
        for column in 0..record_batch.num_columns() {
            let array = record_batch.column(column);

            let value = if array.is_null(row) {
                None
            } else {
                match array.data_type() {
                    DataType::Int8 => arrow_to_string::<Int8Array>(array, row),
                    DataType::Int16 => arrow_to_string::<Int16Array>(array, row),
                    DataType::Int32 => arrow_to_string::<Int32Array>(array, row),
                    DataType::Int64 => arrow_to_string::<Int64Array>(array, row),
                    DataType::Utf8 => arrow_to_string::<StringArray>(array, row),
                    DataType::Float32 => arrow_to_string::<Float32Array>(array, row),
                    DataType::Float64 => arrow_to_string::<Float64Array>(array, row),
                    DataType::Boolean => arrow_to_string::<BooleanArray>(array, row),
                    DataType::Date32 => arrow_date_to_string::<Date32Array>(array, row),
                    DataType::Date64 => arrow_date_to_string::<Date64Array>(array, row),
                    _ => {
                        unsupported.insert(array.data_type());
                        None
                    }
                }
            };
            row_data.push(value);
        }
        data.push(row_data);
    }

    log(&format!("Unsupported type: {:?}", unsupported));

    SerializableRecordBatch { data }
}

/// Convert an Arrow native type to a string
pub fn arrow_to_string<'a, T>(array: &'a ArrayRef, row: usize) -> Option<String>
where
    T: Array + 'static,
    &'a T: ArrayAccessor,
    <&'a T as ArrayAccessor>::Item: std::fmt::Display,
{
    let native_array = array.as_any().downcast_ref::<T>().unwrap();
    Some(native_array.value(row).to_string())
}

pub fn arrow_date_to_string<'a, T>(array: &'a ArrayRef, row: usize) -> Option<String>
where
    T: Array + 'static,
    &'a T: ArrayAccessor,
    <&'a T as ArrayAccessor>::Item: std::fmt::Display,
{
    match array.data_type() {
        DataType::Date32 => {
            let date_array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Some(Date32Type::to_naive_date(date_array.value(row)).to_string())
        }
        DataType::Date64 => {
            let date_array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Some(Date64Type::to_naive_date(date_array.value(row)).to_string())
        }
        _ => None,
    }
}
