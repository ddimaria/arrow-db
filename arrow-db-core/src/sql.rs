//! SQL operations in DataFusion.
//!
//! Before SQL queries can be executed on the database, the tables must be
//! registered with the DataFusion context, which is a cheap operation.

use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    common::DFSchema,
    datasource::MemTable,
    logical_expr::{DmlStatement, LogicalPlan, WriteOp},
    prelude::DataFrame,
};

use crate::{
    database::Database,
    error::{DbError, Result},
    get_table,
    table::Table,
};

impl<'a> Database<'a> {
    /// Register a table with the DataFusion context
    pub fn add_table_context(&self, table_name: &str) -> Result<()> {
        let table = get_table!(self, table_name)?;
        let schema = table.record_batch.schema();
        let provider =
            MemTable::try_new(schema, vec![vec![table.record_batch.to_owned()]]).unwrap();

        self.ctx
            .register_table(table_name, Arc::new(provider))
            .unwrap();

        Ok(())
    }

    /// Register all tables with the DataFusion context
    pub fn add_all_table_contexts(&self) -> Result<()> {
        for table in self.tables.iter() {
            self.add_table_context(&table.key().to_string())?;
        }

        Ok(())
    }

    /// Remove a table from the DataFusion context
    pub fn remove_table_context(&mut self, table: Table<'a>) -> Result<Arc<dyn TableProvider>> {
        let table_name = table.name;
        let provider = self.ctx.deregister_table(table_name).unwrap().unwrap();

        Ok(provider)
    }

    /// Run a SQL query, returning a `DataFrame`
    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

        let logical_plan = df.logical_plan().to_owned();

        let logical_plan = match logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Update,
                input,
                ..
            }) => {
                let name = table_name.table();
                let _session_state = self.ctx.state();
                let table_provider = self.ctx.table_provider(table_name.clone()).await.unwrap();
                let schema_ref = table_provider.schema();
                let df_schema =
                    DFSchema::try_from_qualified_schema(table_name.table(), schema_ref.as_ref())
                        .unwrap();
                println!("{:?} {:?}", name, input);

                LogicalPlan::Dml(DmlStatement::new(
                    table_name,
                    Arc::new(df_schema),
                    WriteOp::Update,
                    input,
                    // Arc::new(project(rename_logical_plan(input, &fields)?, exprs)?),
                ))
            }
            _ => logical_plan,
        };

        let physical_plan = self
            .ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        println!("{:?}", physical_plan);

        Ok(df)
    }

    #[cfg(test)]
    pub async fn test_query(&self, sql: &str) {
        println!("\n{}", sql);
        self.query(sql).await.unwrap().show().await.unwrap();
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
    };

    // use super::*;

    #[tokio::test]
    async fn test_sql() {
        let (mut database, _) = create_database();
        seed_database(&mut database);

        database.print();
        database.add_all_table_contexts().unwrap();

        database
            .test_query("insert into users values (5, 'Eve')")
            .await;

        // database
        //     .test_query("insert into user_role values (5, 'manager')")
        //     .await;

        // database
        //     .test_query("select * from users inner join user_role on users.id = user_role.user_id ")
        //     .await;

        // database
        //     .test_query(
        //         "select * from users inner join user_role on users.id = user_role.user_id
        //         where id > 1
        //         order by name desc",
        //     )
        //     .await;

        database
            .test_query("update users set name = 'Eve2' where id = 5")
            .await;

        // database.test_query("delete from users where id = 5").await;
        // let batch = database.remove_table_context(table).unwrap();
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
