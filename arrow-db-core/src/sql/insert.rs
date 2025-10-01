//! Insert SQL operations in DataFusion.
//!
//!

use datafusion::{
    logical_expr::{Expr, LogicalPlan},
    prelude::DataFrame,
    scalar::ScalarValue,
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

/// Represents an INSERT value for a column
#[derive(Debug, Clone)]
pub struct InsertValue {
    pub column: String,
    pub value: ScalarValue,
}

/// Represents parsed INSERT components
#[derive(Debug, Clone)]
pub struct InsertComponents {
    pub rows: Vec<Vec<InsertValue>>,     // for INSERT VALUES
    pub from_query: Option<LogicalPlan>, // for INSERT FROM SELECT
}

impl<'a> Database<'a> {
    /// Parse INSERT logical plan to extract VALUES
    pub(crate) fn parse_insert_plan(&self, input: &LogicalPlan) -> Result<InsertComponents> {
        match input {
            LogicalPlan::Projection(projection) => {
                // for INSERT, look for a Values node in the input
                if let LogicalPlan::Values(values_node) = projection.input.as_ref() {
                    let mut rows = Vec::new();

                    // process all rows, not just the first one
                    for row_values in &values_node.values {
                        let mut row = Vec::new();

                        // map the values to column names from the projection
                        for (i, expr) in projection.expr.iter().enumerate() {
                            if let Expr::Alias(alias) = expr {
                                let column_name = alias.name.clone();

                                // get the corresponding value from the VALUES clause
                                if let Some(value_expr) = row_values.get(i) {
                                    if let Some(value) = self.extract_scalar_value(value_expr) {
                                        row.push(InsertValue {
                                            column: column_name,
                                            value,
                                        });
                                    }
                                }
                            }
                        }

                        if !row.is_empty() {
                            rows.push(row);
                        }
                    }

                    Ok(InsertComponents {
                        rows,
                        from_query: None,
                    })
                } else {
                    // check if this is INSERT FROM SELECT by looking at the expressions
                    let has_column_expressions = projection.expr.iter().any(|expr| {
                        if let Expr::Alias(alias) = expr {
                            matches!(alias.expr.as_ref(), Expr::Column(_))
                        } else {
                            false
                        }
                    });

                    if has_column_expressions {
                        // this looks like INSERT FROM SELECT - the expressions are column references
                        Ok(InsertComponents {
                            rows: Vec::new(),
                            from_query: Some(projection.input.as_ref().clone()),
                        })
                    } else {
                        // fallback: try to extract from projection expressions directly (single row)
                        let mut row = Vec::new();

                        for expr in &projection.expr {
                            if let Expr::Alias(alias) = expr {
                                let column_name = alias.name.clone();

                                // check if the aliased expression is a literal (INSERT value)
                                if let Some(value) = self.extract_scalar_value(&alias.expr) {
                                    row.push(InsertValue {
                                        column: column_name,
                                        value,
                                    });
                                }
                            }
                        }

                        Ok(InsertComponents {
                            rows: vec![row],
                            from_query: None,
                        })
                    }
                }
            }
            _ => {
                // for now, return empty components if we can't parse the plan
                Ok(InsertComponents {
                    rows: Vec::new(),
                    from_query: None,
                })
            }
        }
    }
    /// Execute INSERT operation on the table data (supports multiple rows and INSERT FROM SELECT)
    pub(crate) async fn execute_insert(
        &self,
        table_name: &str,
        input: &LogicalPlan,
    ) -> Result<usize> {
        // parse the logical plan to extract INSERT values
        let insert_components = self.parse_insert_plan(input)?;

        // handle INSERT FROM SELECT
        if let Some(query_plan) = insert_components.from_query {
            return self
                .execute_insert_from_select(table_name, &query_plan)
                .await;
        }

        if insert_components.rows.is_empty() {
            return Err(DbError::Query(
                "No VALUES found in INSERT".into(),
                "".into(),
            ));
        }

        // get the table schema to determine column order
        let schema = {
            let table = get_table!(self, table_name)?;
            table.record_batch.schema()
        };

        let mut inserted_count = 0;

        // process each row
        for row_values in &insert_components.rows {
            // create row data in schema order
            let mut row_data = Vec::new();
            for field in schema.fields() {
                let column_name = field.name();

                // find the value for this column in the current row
                let insert_value = row_values
                    .iter()
                    .find(|v| v.column == *column_name)
                    .ok_or_else(|| {
                        DbError::Query(
                            format!("No value provided for column '{}'", column_name),
                            "".into(),
                        )
                    })?;

                // convert the scalar value to an ArrayRef
                let array_ref = self.scalar_to_array_ref(&insert_value.value)?;
                row_data.push(array_ref);
            }

            // append the complete row to the table
            let mut table = get_mut_table!(self, table_name)?;
            table.append_row(row_data)?;
            inserted_count += 1;
        }

        Ok(inserted_count)
    }

    /// Execute INSERT FROM SELECT operation
    async fn execute_insert_from_select(
        &self,
        table_name: &str,
        query_plan: &LogicalPlan,
    ) -> Result<usize> {
        // create a DataFrame from the query plan
        let df = DataFrame::new(self.ctx.state(), query_plan.clone());

        // execute the SELECT query to get the data
        let record_batches = df.collect().await.map_err(|e| {
            DbError::Query(
                "Failed to execute SELECT query for INSERT FROM SELECT".into(),
                e.to_string(),
            )
        })?;

        let mut inserted_count = 0;

        // process each record batch
        for batch in record_batches {
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            // get the target table schema
            let target_schema = {
                let table = get_table!(self, table_name)?;
                table.record_batch.schema()
            };

            // process each row in the batch
            for row_idx in 0..num_rows {
                let mut row_data = Vec::new();

                // for each column in the target schema, find the corresponding column in the source
                for (target_col_idx, target_field) in target_schema.fields().iter().enumerate() {
                    let target_column_name = target_field.name();

                    // find the corresponding column in the source batch
                    let source_column = if target_col_idx < num_columns {
                        batch.column(target_col_idx)
                    } else {
                        return Err(DbError::Query(
                            format!(
                                "Source query doesn't have enough columns for target column '{}'",
                                target_column_name
                            ),
                            "".into(),
                        ));
                    };

                    // extract the value from the source column
                    let array_ref =
                        self.extract_single_value_from_column(source_column, row_idx)?;
                    row_data.push(array_ref);
                }

                // append the row to the target table
                let mut table = get_mut_table!(self, table_name)?;
                table.append_row(row_data)?;
                inserted_count += 1;
            }
        }

        Ok(inserted_count)
    }
}

#[cfg(test)]
pub mod tests {

    use arrow::array::{Int32Array, StringArray};

    use crate::{
        database::tests::{create_database, seed_database},
        sql::query::tests::test_query,
    };

    #[tokio::test]
    async fn test_insert_operation() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Test INSERT - just verify it doesn't crash and returns expected result
        let result = database
            .query("insert into users values (5, 'Eve')")
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();

        // Should return a count result
        assert!(!batches.is_empty(), "INSERT should return a result");
    }

    #[tokio::test]
    async fn test_insert_from_select() {
        let (mut database, _) = create_database();
        seed_database(&mut database);
        database.add_all_table_contexts().unwrap();

        // Create a backup_users table with the same schema as users
        let mut backup_users_table = crate::table::Table::new("backup_users");

        // Add the same columns as users table (id: Int32, name: String)
        backup_users_table
            .add_column::<Int32Array>(
                0,
                "id",
                arrow_schema::DataType::Int32,
                Int32Array::from(Vec::<i32>::new()).into(),
            )
            .unwrap();

        backup_users_table
            .add_column::<StringArray>(
                1,
                "name",
                arrow_schema::DataType::Utf8,
                StringArray::from(Vec::<String>::new()).into(),
            )
            .unwrap();

        database.add_table(backup_users_table).unwrap();
        database.add_table_context("backup_users").unwrap();

        // Initial state: users table has 4 users, backup_users is empty
        test_query(
            &database,
            "SELECT * FROM users",
            4,
            "Should have 4 users initially",
        )
        .await;

        test_query(
            &database,
            "SELECT * FROM backup_users",
            0,
            "backup_users should be empty initially",
        )
        .await;

        // Test INSERT FROM SELECT - copy all users to backup
        database
            .query("INSERT INTO backup_users SELECT * FROM users")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM backup_users",
            4,
            "backup_users should have 4 users after INSERT FROM SELECT",
        )
        .await;

        // Test INSERT FROM SELECT with WHERE clause - copy only specific users
        database
            .query("INSERT INTO backup_users SELECT * FROM users WHERE id > 2")
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM backup_users",
            6,
            "backup_users should have 6 users after selective INSERT FROM SELECT",
        )
        .await;

        // Test INSERT FROM SELECT with complex conditions
        // This should insert: Alice (name LIKE 'A%') + Bob (id=2) + David (id=4) = 3 users
        // Total: 4 + 2 + 3 = 9 users
        database
            .query(
                "INSERT INTO backup_users SELECT * FROM users WHERE name LIKE 'A%' OR id IN (2, 4)",
            )
            .await
            .unwrap();

        test_query(
            &database,
            "SELECT * FROM backup_users",
            9,
            "backup_users should have 9 users after complex INSERT FROM SELECT",
        )
        .await;
    }
}
