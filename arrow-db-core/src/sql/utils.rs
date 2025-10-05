//! Util SQL operations in DataFusion.
//!
//!

use std::sync::Arc;

use datafusion::{
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator},
    scalar::ScalarValue,
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, StringArray,
};

use crate::{
    database::Database,
    error::{DbError, Result},
    get_table,
};

impl<'a> Database<'a> {
    /// Extract scalar value from an expression
    pub(crate) fn extract_scalar_value(&self, expr: &Expr) -> Option<ScalarValue> {
        match expr {
            Expr::Literal(scalar) => Some(scalar.clone()),
            Expr::Column(_) => None, // column references don't have scalar values
            _ => None,               // for now, only handle literals
        }
    }

    /// Extract WHERE condition from a logical plan
    pub(crate) fn extract_where_condition(plan: &LogicalPlan) -> Result<Option<Expr>> {
        match plan {
            // primary filter node - contains the WHERE condition
            LogicalPlan::Filter(filter) => Ok(Some(filter.predicate.clone())),

            // base table scan - no WHERE condition
            LogicalPlan::TableScan(_) => Ok(None),

            // projection - check input for WHERE conditions
            LogicalPlan::Projection(projection) => Self::extract_where_condition(&projection.input),

            // sort - check input for WHERE conditions (ORDER BY can have WHERE)
            LogicalPlan::Sort(sort) => Self::extract_where_condition(&sort.input),

            // limit - check input for WHERE conditions (LIMIT can have WHERE)
            LogicalPlan::Limit(limit) => Self::extract_where_condition(&limit.input),

            // aggregate - check input for WHERE conditions (GROUP BY can have WHERE)
            LogicalPlan::Aggregate(aggregate) => Self::extract_where_condition(&aggregate.input),

            // distinct - check input for WHERE conditions
            LogicalPlan::Distinct(_) => {
                // use the generic inputs() method
                if let Some(input) = plan.inputs().first() {
                    Self::extract_where_condition(input)
                } else {
                    Ok(None)
                }
            }

            // join - check both inputs for WHERE conditions, prefer left side
            LogicalPlan::Join(_) => {
                let inputs = plan.inputs();
                // first check left input (index 0)
                if let Some(left_input) = inputs.first() {
                    if let Some(condition) = Self::extract_where_condition(left_input)? {
                        return Ok(Some(condition));
                    }
                }
                // then check right input (index 1)
                if let Some(right_input) = inputs.get(1) {
                    Self::extract_where_condition(right_input)
                } else {
                    Ok(None)
                }
            }

            // union - check first input for WHERE conditions
            LogicalPlan::Union(_) => {
                if let Some(first_input) = plan.inputs().first() {
                    Self::extract_where_condition(first_input)
                } else {
                    Ok(None)
                }
            }

            // subquery - check the subquery plan
            LogicalPlan::SubqueryAlias(_) => {
                if let Some(input) = plan.inputs().first() {
                    Self::extract_where_condition(input)
                } else {
                    Ok(None)
                }
            }

            // for any other plan types, recursively check the first input
            _ => {
                if let Some(input) = plan.inputs().first() {
                    Self::extract_where_condition(input)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Evaluate a WHERE condition against a row
    pub(crate) fn evaluate_where_condition(
        &self,
        condition: &Expr,
        table_name: &str,
        row_idx: usize,
    ) -> Result<bool> {
        match condition {
            // handle IN clauses like "id IN (1, 2, 3)"
            Expr::InList(in_list) => {
                if let Expr::Column(col_ref) = in_list.expr.as_ref() {
                    let column_name = col_ref.name.as_str();
                    let column_value = self.get_column_value(table_name, column_name, row_idx)?;

                    let mut found = false;
                    for list_expr in &in_list.list {
                        if let Some(list_value) = self.extract_scalar_value(list_expr) {
                            if self.scalar_values_equal(&column_value, &list_value) {
                                found = true;
                                break;
                            }
                        }
                    }

                    return Ok(if in_list.negated { !found } else { found });
                }
            }

            // handle LIKE patterns like "name LIKE 'A%'"
            Expr::Like(like_expr) => {
                if let (
                    Expr::Column(col_ref),
                    Expr::Literal(ScalarValue::Utf8(Some(pattern_str))),
                ) = (like_expr.expr.as_ref(), like_expr.pattern.as_ref())
                {
                    let column_name = col_ref.name.as_str();
                    if let Some(ScalarValue::Utf8(Some(actual_str))) =
                        self.get_column_value(table_name, column_name, row_idx)?
                    {
                        let matches = self.matches_like_pattern(&actual_str, pattern_str);
                        return Ok(if like_expr.negated { !matches } else { matches });
                    }
                }
            }

            // handle IS NULL
            Expr::IsNull(expr) => {
                if let Expr::Column(col_ref) = expr.as_ref() {
                    let column_name = col_ref.name.as_str();
                    let is_null = self.is_column_null(table_name, column_name, row_idx)?;
                    return Ok(is_null);
                }
            }

            // handle IS NOT NULL
            Expr::IsNotNull(expr) => {
                if let Expr::Column(col_ref) = expr.as_ref() {
                    let column_name = col_ref.name.as_str();
                    let is_null = self.is_column_null(table_name, column_name, row_idx)?;
                    return Ok(!is_null);
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match op {
                    // comparison operators - all follow the same pattern
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {
                        if let (Expr::Column(col_ref), Expr::Literal(value)) =
                            (left.as_ref(), right.as_ref())
                        {
                            let column_name = col_ref.name.as_str();
                            return self.check_column_comparison(
                                table_name,
                                column_name,
                                row_idx,
                                value,
                                op,
                            );
                        }
                    }
                    // logical operators
                    Operator::And => {
                        let left_result =
                            self.evaluate_where_condition(left, table_name, row_idx)?;
                        let right_result =
                            self.evaluate_where_condition(right, table_name, row_idx)?;
                        return Ok(left_result && right_result);
                    }
                    Operator::Or => {
                        let left_result =
                            self.evaluate_where_condition(left, table_name, row_idx)?;
                        let right_result =
                            self.evaluate_where_condition(right, table_name, row_idx)?;
                        return Ok(left_result || right_result);
                    }
                    _ => return Ok(false), // unsupported operator
                }
            }
            _ => return Ok(false), // unsupported condition type for now
        }

        Ok(false)
    }

    /// Check if a column value compares to a scalar value at a specific row using the given operator
    pub(crate) fn check_column_comparison(
        &self,
        table_name: &str,
        column_name: &str,
        row_idx: usize,
        expected_value: &ScalarValue,
        operator: &Operator,
    ) -> Result<bool> {
        let table = get_table!(self, table_name)?;
        let schema = table.record_batch.schema();

        let column_index = schema
            .column_with_name(column_name)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DbError::Query(format!("Column '{}' not found", column_name), "".into())
            })?;

        let column = table.record_batch.column(column_index);

        match expected_value {
            ScalarValue::Int32(Some(expected_int)) => {
                if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                    if row_idx < int_array.len() && !int_array.is_null(row_idx) {
                        let actual_value = int_array.value(row_idx);
                        return Ok(self.compare_values(&actual_value, expected_int, operator));
                    }
                }
            }
            ScalarValue::Int64(Some(expected_int)) => {
                // handle Int64 literals that should match Int32 columns
                if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                    if row_idx < int_array.len() && !int_array.is_null(row_idx) {
                        let actual_value = int_array.value(row_idx) as i64;
                        return Ok(self.compare_values(&actual_value, expected_int, operator));
                    }
                }
            }
            ScalarValue::Float32(Some(expected_float)) => {
                if let Some(float_array) = column.as_any().downcast_ref::<Float32Array>() {
                    if row_idx < float_array.len() && !float_array.is_null(row_idx) {
                        let actual_value = float_array.value(row_idx);
                        return Ok(self.compare_float_values(
                            actual_value,
                            *expected_float,
                            operator,
                        ));
                    }
                }
            }
            ScalarValue::Float64(Some(expected_float)) => {
                if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                    if row_idx < float_array.len() && !float_array.is_null(row_idx) {
                        let actual_value = float_array.value(row_idx);
                        return Ok(self.compare_float_values(
                            actual_value,
                            *expected_float,
                            operator,
                        ));
                    }
                }
                // also handle Float32 columns with Float64 literals
                if let Some(float_array) = column.as_any().downcast_ref::<Float32Array>() {
                    if row_idx < float_array.len() && !float_array.is_null(row_idx) {
                        let actual_value = float_array.value(row_idx) as f64;
                        return Ok(self.compare_float_values(
                            actual_value,
                            *expected_float,
                            operator,
                        ));
                    }
                }
            }
            ScalarValue::Boolean(Some(expected_bool)) => {
                if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                    if row_idx < bool_array.len() && !bool_array.is_null(row_idx) {
                        let actual_value = bool_array.value(row_idx);
                        return Ok(self.compare_values(&actual_value, expected_bool, operator));
                    }
                }
            }
            ScalarValue::Date32(Some(expected_date)) => {
                if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    if row_idx < date_array.len() && !date_array.is_null(row_idx) {
                        let actual_value = date_array.value(row_idx);
                        return Ok(self.compare_values(&actual_value, expected_date, operator));
                    }
                }
            }
            ScalarValue::Utf8(Some(expected_str)) => {
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    if row_idx < string_array.len() && !string_array.is_null(row_idx) {
                        let actual_value = string_array.value(row_idx);
                        return Ok(self.compare_values(
                            &actual_value,
                            &expected_str.as_str(),
                            operator,
                        ));
                    }
                }
            }
            _ => return Ok(false), // unsupported data type
        }

        Ok(false)
    }

    /// Generic comparison function that works with any comparable type
    pub(crate) fn compare_values<T: PartialOrd + PartialEq>(
        &self,
        actual: &T,
        expected: &T,
        operator: &Operator,
    ) -> bool {
        match operator {
            Operator::Eq => actual == expected,
            Operator::NotEq => actual != expected,
            Operator::Lt => actual < expected,
            Operator::LtEq => actual <= expected,
            Operator::Gt => actual > expected,
            Operator::GtEq => actual >= expected,
            _ => false, // unsupported operator for comparison
        }
    }

    /// Specialized comparison function for floating point numbers with epsilon tolerance
    pub(crate) fn compare_float_values<T: Into<f64> + Copy>(
        &self,
        actual: T,
        expected: T,
        operator: &Operator,
    ) -> bool {
        let actual_f64: f64 = actual.into();
        let expected_f64: f64 = expected.into();
        let epsilon = f64::EPSILON * 1000.0; // use a reasonable epsilon for comparisons

        match operator {
            Operator::Eq => (actual_f64 - expected_f64).abs() < epsilon,
            Operator::NotEq => (actual_f64 - expected_f64).abs() >= epsilon,
            Operator::Lt => actual_f64 < expected_f64 - epsilon,
            Operator::LtEq => actual_f64 <= expected_f64 + epsilon,
            Operator::Gt => actual_f64 > expected_f64 + epsilon,
            Operator::GtEq => actual_f64 >= expected_f64 - epsilon,
            _ => false, // unsupported operator for comparison
        }
    }

    /// Get the scalar value of a column at a specific row
    pub(crate) fn get_column_value(
        &self,
        table_name: &str,
        column_name: &str,
        row_idx: usize,
    ) -> Result<Option<ScalarValue>> {
        let table = get_table!(self, table_name)?;
        let schema = table.record_batch.schema();

        let column_index = schema
            .column_with_name(column_name)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DbError::Query(format!("Column '{}' not found", column_name), "".into())
            })?;

        let column = table.record_batch.column(column_index);

        if row_idx >= column.len() {
            return Ok(None);
        }

        if column.is_null(row_idx) {
            return Ok(None);
        }

        // extract the actual value based on the column type
        match column.data_type() {
            arrow::datatypes::DataType::Int32 => {
                if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                    Ok(Some(ScalarValue::Int32(Some(int_array.value(row_idx)))))
                } else {
                    Ok(None)
                }
            }
            arrow::datatypes::DataType::Float32 => {
                if let Some(float_array) = column.as_any().downcast_ref::<Float32Array>() {
                    Ok(Some(ScalarValue::Float32(Some(float_array.value(row_idx)))))
                } else {
                    Ok(None)
                }
            }
            arrow::datatypes::DataType::Float64 => {
                if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                    Ok(Some(ScalarValue::Float64(Some(float_array.value(row_idx)))))
                } else {
                    Ok(None)
                }
            }
            arrow::datatypes::DataType::Boolean => {
                if let Some(bool_array) = column.as_any().downcast_ref::<BooleanArray>() {
                    Ok(Some(ScalarValue::Boolean(Some(bool_array.value(row_idx)))))
                } else {
                    Ok(None)
                }
            }
            arrow::datatypes::DataType::Date32 => {
                if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    Ok(Some(ScalarValue::Date32(Some(date_array.value(row_idx)))))
                } else {
                    Ok(None)
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    Ok(Some(ScalarValue::Utf8(Some(
                        string_array.value(row_idx).to_string(),
                    ))))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None), // unsupported data type
        }
    }

    /// Check if two scalar values are equal
    pub(crate) fn scalar_values_equal(&self, a: &Option<ScalarValue>, b: &ScalarValue) -> bool {
        match (a, b) {
            // Integer comparisons
            (Some(ScalarValue::Int32(Some(a_val))), ScalarValue::Int32(Some(b_val))) => {
                a_val == b_val
            }
            (Some(ScalarValue::Int32(Some(a_val))), ScalarValue::Int64(Some(b_val))) => {
                *a_val as i64 == *b_val
            }

            // Float comparisons
            (Some(ScalarValue::Float32(Some(a_val))), ScalarValue::Float32(Some(b_val))) => {
                (a_val - b_val).abs() < f32::EPSILON
            }
            (Some(ScalarValue::Float64(Some(a_val))), ScalarValue::Float64(Some(b_val))) => {
                (a_val - b_val).abs() < f64::EPSILON
            }
            (Some(ScalarValue::Float32(Some(a_val))), ScalarValue::Float64(Some(b_val))) => {
                (*a_val as f64 - b_val).abs() < f64::EPSILON
            }
            (Some(ScalarValue::Float64(Some(a_val))), ScalarValue::Float32(Some(b_val))) => {
                (a_val - *b_val as f64).abs() < f64::EPSILON
            }

            // Boolean comparisons
            (Some(ScalarValue::Boolean(Some(a_val))), ScalarValue::Boolean(Some(b_val))) => {
                a_val == b_val
            }

            // Date comparisons
            (Some(ScalarValue::Date32(Some(a_val))), ScalarValue::Date32(Some(b_val))) => {
                a_val == b_val
            }

            // String comparisons
            (Some(ScalarValue::Utf8(Some(a_val))), ScalarValue::Utf8(Some(b_val))) => {
                a_val == b_val
            }

            _ => false,
        }
    }

    /// Check if a column value is null at a specific row
    pub(crate) fn is_column_null(
        &self,
        table_name: &str,
        column_name: &str,
        row_idx: usize,
    ) -> Result<bool> {
        let table = get_table!(self, table_name)?;
        let schema = table.record_batch.schema();

        let column_index = schema
            .column_with_name(column_name)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DbError::Query(format!("Column '{}' not found", column_name), "".into())
            })?;

        let column = table.record_batch.column(column_index);

        if row_idx >= column.len() {
            return Ok(true); // out of bounds is considered null
        }

        Ok(column.is_null(row_idx))
    }

    /// Check if a string matches a LIKE pattern (supports % and _ wildcards)
    pub(crate) fn matches_like_pattern(&self, text: &str, pattern: &str) -> bool {
        // simple LIKE pattern matching without regex dependency
        Self::simple_like_match(text, pattern, 0, 0)
    }

    /// Simple LIKE pattern matching implementation
    pub(crate) fn simple_like_match(
        text: &str,
        pattern: &str,
        text_idx: usize,
        pattern_idx: usize,
    ) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();

        if pattern_idx >= pattern_chars.len() {
            return text_idx >= text_chars.len();
        }

        if text_idx >= text_chars.len() {
            // check if remaining pattern is all '%'
            return pattern_chars[pattern_idx..].iter().all(|&c| c == '%');
        }

        match pattern_chars[pattern_idx] {
            '%' => {
                // % matches zero or more characters
                // try matching with current position or advancing text
                Self::simple_like_match(text, pattern, text_idx, pattern_idx + 1)
                    || Self::simple_like_match(text, pattern, text_idx + 1, pattern_idx)
            }
            '_' => {
                // _ matches exactly one character
                Self::simple_like_match(text, pattern, text_idx + 1, pattern_idx + 1)
            }
            c => {
                // regular character must match exactly
                if text_chars[text_idx] == c {
                    Self::simple_like_match(text, pattern, text_idx + 1, pattern_idx + 1)
                } else {
                    false
                }
            }
        }
    }

    /// Extract a single value from a column at a specific row and convert to ArrayRef
    pub(crate) fn extract_single_value_from_column(
        &self,
        column: &ArrayRef,
        row_idx: usize,
    ) -> Result<ArrayRef> {
        if row_idx >= column.len() {
            return Err(DbError::Query("Row index out of bounds".into(), "".into()));
        }

        match column.data_type() {
            arrow::datatypes::DataType::Int32 => {
                if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
                    let value = if int_array.is_null(row_idx) {
                        None
                    } else {
                        Some(int_array.value(row_idx))
                    };
                    Ok(Arc::new(Int32Array::from(vec![value])))
                } else {
                    Err(DbError::Query(
                        "Failed to downcast Int32Array".into(),
                        "".into(),
                    ))
                }
            }
            arrow::datatypes::DataType::Float64 => {
                if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
                    let value = if float_array.is_null(row_idx) {
                        None
                    } else {
                        Some(float_array.value(row_idx))
                    };
                    Ok(Arc::new(Float64Array::from(vec![value])))
                } else {
                    Err(DbError::Query(
                        "Failed to downcast Float64Array".into(),
                        "".into(),
                    ))
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    let value = if string_array.is_null(row_idx) {
                        None
                    } else {
                        Some(string_array.value(row_idx))
                    };
                    Ok(Arc::new(StringArray::from(vec![value])))
                } else {
                    Err(DbError::Query(
                        "Failed to downcast StringArray".into(),
                        "".into(),
                    ))
                }
            }
            _ => Err(DbError::Query(
                format!(
                    "Unsupported data type for INSERT FROM SELECT: {:?}",
                    column.data_type()
                ),
                "".into(),
            )),
        }
    }

    /// Convert a ScalarValue to an ArrayRef with a single element
    pub(crate) fn scalar_to_array_ref(&self, value: &ScalarValue) -> Result<ArrayRef> {
        match value {
            ScalarValue::Int32(Some(val)) => Ok(Arc::new(Int32Array::from(vec![*val]))),
            ScalarValue::Int64(Some(val)) => {
                // convert Int64 to Int32 for compatibility
                Ok(Arc::new(Int32Array::from(vec![*val as i32])))
            }
            ScalarValue::Float32(Some(val)) => Ok(Arc::new(Float32Array::from(vec![*val]))),
            ScalarValue::Float64(Some(val)) => Ok(Arc::new(Float64Array::from(vec![*val]))),
            ScalarValue::Boolean(Some(val)) => Ok(Arc::new(BooleanArray::from(vec![*val]))),
            ScalarValue::Date32(Some(val)) => Ok(Arc::new(Date32Array::from(vec![*val]))),
            ScalarValue::Utf8(Some(val)) => Ok(Arc::new(StringArray::from(vec![val.clone()]))),
            _ => Err(DbError::Query(
                "Unsupported data type for INSERT".into(),
                "".into(),
            )),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Instant;

    use crate::{
        database::{
            tests::{create_database, seed_database},
            Database,
        },
        get_table,
        sql::query::tests::test_query,
    };
    use arrow::array::{Float64Array, Int32Array, StringArray};

    #[tokio::test]
    async fn test_persistence_across_operations() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Initial count should be 4 users
        let result = database
            .query("SELECT COUNT(*) as count FROM users")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        let count_before = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count_before, 4, "Should have 4 initial users");

        // Insert a new user
        database
            .query("INSERT INTO users VALUES (5, 'Eve')")
            .await
            .unwrap();

        // Verify the insert persisted by counting again
        let result = database
            .query("SELECT COUNT(*) as count FROM users")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        let count_after_insert = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count_after_insert, 5, "Should have 5 users after insert");

        // Update the new user
        database
            .query("UPDATE users SET name = 'Eve Updated' WHERE id = 5")
            .await
            .unwrap();

        // Verify the update persisted
        let result = database
            .query("SELECT name FROM users WHERE id = 5")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        let updated_name = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(updated_name, "Eve Updated", "Name should be updated");

        // Delete the user
        database
            .query("DELETE FROM users WHERE id = 5")
            .await
            .unwrap();

        // Verify the delete persisted
        let result = database
            .query("SELECT COUNT(*) as count FROM users")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        let count_after_delete = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            count_after_delete, 4,
            "Should be back to 4 users after delete"
        );
    }

    #[tokio::test]
    async fn test_where_clause_operators() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test all comparison operators
        test_query(
            &database,
            "SELECT * FROM users WHERE id = 2",
            1,
            "Should find 1 user with id = 2",
        )
        .await;
        test_query(
            &database,
            "SELECT * FROM users WHERE id != 2",
            3,
            "Should find 3 users with id != 2",
        )
        .await;
        test_query(
            &database,
            "SELECT * FROM users WHERE id > 2",
            2,
            "Should find 2 users with id > 2",
        )
        .await;
        test_query(
            &database,
            "SELECT * FROM users WHERE id >= 2",
            3,
            "Should find 3 users with id >= 2",
        )
        .await;
        test_query(
            &database,
            "SELECT * FROM users WHERE id < 3",
            2,
            "Should find 2 users with id < 3",
        )
        .await;
        test_query(
            &database,
            "SELECT * FROM users WHERE id <= 3",
            3,
            "Should find 3 users with id <= 3",
        )
        .await;

        // Test string comparisons
        let result = database
            .query("SELECT * FROM users WHERE name > 'Alice'")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        assert!(
            batches[0].num_rows() >= 1,
            "Should find users with name > 'Alice'"
        );
    }

    #[tokio::test]
    async fn test_logical_operators() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test logical operators
        test_query(
            &database,
            "SELECT * FROM users WHERE id > 1 AND id < 4",
            2,
            "Should find 2 users with id between 1 and 4",
        )
        .await;

        test_query(
            &database,
            "SELECT * FROM users WHERE id = 1 OR id = 4",
            2,
            "Should find 2 users with id = 1 OR id = 4",
        )
        .await;

        test_query(
            &database,
            "SELECT * FROM users WHERE (id = 1 OR id = 2) AND name != 'Charlie'",
            2,
            "Should find users matching complex condition",
        )
        .await;
    }

    #[tokio::test]
    async fn test_complex_logical_plans() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test queries that generate complex logical plans

        // ORDER BY with WHERE (Sort + Filter + TableScan)
        test_query(
            &database,
            "SELECT * FROM users WHERE id > 1 ORDER BY name",
            3,
            "Should find 3 users with id > 1 ordered by name",
        )
        .await;

        // LIMIT with WHERE (Limit + Filter + TableScan)
        test_query(
            &database,
            "SELECT * FROM users WHERE id < 4 LIMIT 2",
            2,
            "Should find 2 users with id < 4 limited to 2 rows",
        )
        .await;

        // Note: DISTINCT queries may not work with our custom DML implementation
        // as they generate complex plans that DataFusion handles differently
        // This is expected behavior - our DML is for UPDATE/INSERT/DELETE operations

        // Complex projection with WHERE (Projection + Filter + TableScan)
        test_query(
            &database,
            "SELECT name, id FROM users WHERE name != 'Alice'",
            3,
            "Should find 3 users where name is not Alice",
        )
        .await;

        // Combined ORDER BY and LIMIT with WHERE
        test_query(
            &database,
            "SELECT * FROM users WHERE id > 1 ORDER BY id LIMIT 2",
            2,
            "Should find 2 users with id > 1, ordered and limited",
        )
        .await;
    }

    #[tokio::test]
    async fn test_enhanced_expressions() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test IN clauses with SELECT to verify they work
        test_query(
            &database,
            "SELECT * FROM users WHERE id IN (1, 3)",
            2,
            "Should find 2 users with id IN (1, 3)",
        )
        .await;

        // Test NOT IN clauses
        test_query(
            &database,
            "SELECT * FROM users WHERE id NOT IN (1, 2)",
            2,
            "Should find 2 users with id NOT IN (1, 2)",
        )
        .await;

        // Test LIKE patterns
        test_query(
            &database,
            "SELECT * FROM users WHERE name LIKE 'A%'",
            1,
            "Should find 1 user with name starting with 'A'",
        )
        .await;

        // Test LIKE with underscore wildcard
        test_query(
            &database,
            "SELECT * FROM users WHERE name LIKE 'B_b'",
            1,
            "Should find 1 user with name matching 'B_b' pattern",
        )
        .await;

        // Test IS NOT NULL (all current users have non-null names)
        test_query(
            &database,
            "SELECT * FROM users WHERE name IS NOT NULL",
            4,
            "Should find 4 users with non-NULL names",
        )
        .await;

        // Note: IS NULL testing requires NULL value support in INSERT, which is a future enhancement
    }

    #[tokio::test]
    async fn test_float_data_types() {
        use arrow_schema::DataType;

        let (mut database, _) = create_database();

        // Create a simple prices table with just Float64
        let mut prices_table = crate::table::Table::new("prices");

        prices_table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3]).into(),
            )
            .unwrap();

        prices_table
            .add_column::<Float64Array>(
                1,
                "price",
                DataType::Float64,
                Float64Array::from(vec![999.99, 599.99, 299.99]).into(),
            )
            .unwrap();

        database.add_table(prices_table).unwrap();
        database.add_table_context("prices").unwrap();

        // Test Float comparisons
        test_query(
            &database,
            "SELECT * FROM prices WHERE price > 500.0",
            2,
            "Should find 2 prices > 500.0",
        )
        .await;

        test_query(
            &database,
            "SELECT * FROM prices WHERE price <= 599.99",
            2,
            "Should find 2 prices <= 599.99",
        )
        .await;

        // Test INSERT with Float
        database
            .query("INSERT INTO prices VALUES (4, 399.99)")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM prices",
            4,
            "Should have 4 prices after insert",
        )
        .await;

        // Test UPDATE with Float
        database
            .query("UPDATE prices SET price = 349.99 WHERE id = 4")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM prices WHERE id = 4 AND price < 350.0",
            1,
            "Should find the updated price",
        )
        .await;
    }

    #[tokio::test]
    async fn test_multi_row_operations() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Initial state: should have 4 users
        test_query(
            &database,
            "SELECT * FROM users",
            4,
            "Should start with 4 users",
        )
        .await;

        // Test multi-row INSERT
        database
            .query("INSERT INTO users VALUES (5, 'Eve'), (6, 'Frank'), (7, 'Grace')")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM users",
            7,
            "Should have 7 users after multi-row insert",
        )
        .await;

        // Test that all inserted users are there
        test_query(
            &database,
            "SELECT * FROM users WHERE id IN (5, 6, 7)",
            3,
            "Should find all 3 newly inserted users",
        )
        .await;

        // Test multi-row DELETE with IN clause
        database
            .query("DELETE FROM users WHERE id IN (5, 6)")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM users",
            5,
            "Should have 5 users after deleting 2",
        )
        .await;

        // Test batch UPDATE with range condition
        database
            .query("UPDATE users SET name = 'Updated' WHERE id >= 3")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM users WHERE name = 'Updated'",
            3,
            "Should find 3 users with updated names (ids 3, 4, 7)",
        )
        .await;
    }

    #[tokio::test]
    async fn test_comprehensive_integration() {
        use arrow_schema::DataType;

        let (mut database, _) = create_database();

        // Products table with various data types
        let mut products_table = crate::table::Table::new("products");
        products_table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 3, 4, 5]).into(),
            )
            .unwrap();
        products_table
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(vec!["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]).into(),
            )
            .unwrap();
        products_table
            .add_column::<Float64Array>(
                2,
                "price",
                DataType::Float64,
                Float64Array::from(vec![999.99, 599.99, 299.99, 399.99, 79.99]).into(),
            )
            .unwrap();
        products_table
            .add_column::<StringArray>(
                3,
                "category",
                DataType::Utf8,
                StringArray::from(vec![
                    "Electronics",
                    "Electronics",
                    "Electronics",
                    "Electronics",
                    "Accessories",
                ])
                .into(),
            )
            .unwrap();

        // Orders table
        let mut orders_table = crate::table::Table::new("orders");
        orders_table
            .add_column::<Int32Array>(
                0,
                "order_id",
                DataType::Int32,
                Int32Array::from(vec![101, 102, 103]).into(),
            )
            .unwrap();
        orders_table
            .add_column::<Int32Array>(
                1,
                "product_id",
                DataType::Int32,
                Int32Array::from(vec![1, 2, 1]).into(),
            )
            .unwrap();
        orders_table
            .add_column::<Int32Array>(
                2,
                "quantity",
                DataType::Int32,
                Int32Array::from(vec![2, 1, 1]).into(),
            )
            .unwrap();

        // Archive table (initially empty)
        let mut archive_table = crate::table::Table::new("archive_products");
        archive_table
            .add_column::<Int32Array>(
                0,
                "id",
                DataType::Int32,
                Int32Array::from(Vec::<i32>::new()).into(),
            )
            .unwrap();
        archive_table
            .add_column::<StringArray>(
                1,
                "name",
                DataType::Utf8,
                StringArray::from(Vec::<String>::new()).into(),
            )
            .unwrap();
        archive_table
            .add_column::<Float64Array>(
                2,
                "price",
                DataType::Float64,
                Float64Array::from(Vec::<f64>::new()).into(),
            )
            .unwrap();
        archive_table
            .add_column::<StringArray>(
                3,
                "category",
                DataType::Utf8,
                StringArray::from(Vec::<String>::new()).into(),
            )
            .unwrap();

        database.add_table(products_table).unwrap();
        database.add_table(orders_table).unwrap();
        database.add_table(archive_table).unwrap();
        database.add_all_table_contexts().unwrap();

        // Test 1: Enhanced WHERE clauses with multiple operators
        // Products: Laptop(999.99), Phone(599.99), Tablet(299.99), Monitor(399.99), Keyboard(79.99)
        // Electronics over $300: Laptop, Phone, Monitor = 3 products (Tablet is 299.99 < 300)
        test_query(
            &database,
            "SELECT * FROM products WHERE price > 300.0 AND category = 'Electronics'",
            3,
            "Should find 3 electronics products over $300",
        )
        .await;

        // Test 2: IN clauses with complex conditions
        // IDs 1,3,5 = Laptop(999.99), Tablet(299.99), Keyboard(79.99)
        // price <= 500.0 = Tablet, Keyboard = 2 products
        test_query(
            &database,
            "SELECT * FROM products WHERE id IN (1, 3, 5) AND price <= 500.0",
            2,
            "Should find 2 products matching ID and price criteria",
        )
        .await;

        // Test 3: LIKE patterns with logical operators
        test_query(
            &database,
            "SELECT * FROM products WHERE name LIKE '%top' OR name LIKE 'K%'",
            2,
            "Should find products with names ending in 'top' or starting with 'K'",
        )
        .await;

        // Test 4: Multi-row INSERT with various data types
        database
            .query(
                "INSERT INTO products VALUES 
             (6, 'Mouse', 29.99, 'Accessories'),
             (7, 'Webcam', 89.99, 'Electronics'),
             (8, 'Speakers', 149.99, 'Electronics')",
            )
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM products",
            8,
            "Should have 8 products after multi-row insert",
        )
        .await;

        // Test 5: INSERT FROM SELECT with complex WHERE clause
        database
            .query(
                "INSERT INTO archive_products 
             SELECT * FROM products 
             WHERE price < 100.0 AND (category = 'Accessories' OR name LIKE '%board')",
            )
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM archive_products",
            2,
            "Should have 2 archived products (Mouse + Keyboard)",
        )
        .await;

        // Test 6: Batch UPDATE with literal values and complex conditions
        database
            .query(
                "UPDATE products 
             SET price = 199.99 
             WHERE category = 'Electronics' AND price > 500.0",
            )
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM products WHERE price = 199.99",
            2,
            "Should find 2 products updated to $199.99",
        )
        .await;

        // Test 7: Complex DELETE with multiple conditions
        database
            .query(
                "DELETE FROM products 
             WHERE (price < 50.0 OR name LIKE '%cam') AND category IS NOT NULL",
            )
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM products",
            6,
            "Should have 6 products after conditional delete",
        )
        .await;

        // Test 8: Data migration with INSERT FROM SELECT and filtering
        database
            .query(
                "INSERT INTO archive_products 
             SELECT * FROM products 
             WHERE price > 300.0 AND name NOT IN ('Laptop', 'Phone')",
            )
            .await
            .unwrap();

        // Test 9: Verify persistence across multiple operations
        // Archive has: 2 (from step 5) + 1 (from step 8) = 3 products
        test_query(
            &database,
            "SELECT * FROM archive_products",
            3,
            "Archive should have 3 products after all operations",
        )
        .await;

        // Test 10: Complex query combining all features (using >= and <= instead of BETWEEN)
        test_query(
            &database,
            "SELECT * FROM products 
             WHERE price >= 100.0 AND price <= 600.0 
             AND category IN ('Electronics', 'Accessories') 
             AND name NOT LIKE '%Mouse%'
             AND id IS NOT NULL",
            5,
            "Should find 5 products matching complex multi-feature criteria",
        )
        .await;
    }

    #[tokio::test]
    async fn test_benchmark_sql_on_large_db() {
        let now = Instant::now();
        let database = Database::new_from_disk("LargeDB").await.unwrap();
        let elapsed = now.elapsed();

        let rows = get_table!(database, "flights_1m")
            .unwrap()
            .record_batch
            .num_rows();
        let cols = get_table!(database, "flights_1m")
            .unwrap()
            .record_batch
            .num_columns();

        println!("Loaded {} rows and {} cols in {:.2?}", rows, cols, elapsed);

        let now = Instant::now();
        database.add_all_table_contexts().unwrap();
        let elapsed = now.elapsed();

        println!(
            "Added {} rows and {} cols into context in {:.2?}",
            rows, cols, elapsed
        );

        let now = Instant::now();
        database.test_query(
            "select * from flights_1m where flights_1m.\"DISTANCE\" > 1000 and flights_1m.\"DISTANCE\" < 3000 limit 100")
            .await;
        let elapsed = now.elapsed();

        println!(
            "Queried 10 rows from {} rows and {} cols in {:.2?}",
            rows, cols, elapsed
        );
    }
}
