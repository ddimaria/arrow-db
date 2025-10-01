//! Context SQL operations in DataFusion.
//!
//! Before SQL queries can be executed on the database, the tables must be
//! registered with the DataFusion context, which is a cheap operation.

use datafusion::{catalog::TableProvider, datasource::MemTable};
use std::sync::Arc;

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

        // first try to deregister if it exists, then register
        let _ = self.ctx.deregister_table(table_name);

        self.ctx
            .register_table(table_name, Arc::new(provider))
            .map_err(|e| {
                DbError::Query(
                    format!("Failed to register table {}", table_name),
                    e.to_string(),
                )
            })?;

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
}
