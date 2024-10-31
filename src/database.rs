use std::{fmt::Debug, sync::Arc};

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use datafusion::{catalog::TableProvider, datasource::MemTable, prelude::SessionContext};

use crate::{
    error::{DbError, Result},
    table::Table,
};

#[derive(Clone)]
pub struct Database<'a> {
    pub name: &'a str,
    pub tables: DashMap<&'a str, Table<'a>>,
    pub ctx: SessionContext,
}

impl Debug for Database<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .field("tables", &self.tables)
            .finish()
    }
}

impl<'a> Database<'a> {
    pub fn new(name: &'a str) -> Database<'a> {
        Database {
            name,
            tables: DashMap::new(),
            ctx: SessionContext::new(),
        }
    }

    pub fn add_table(&mut self, table: Table<'a>) -> Result<()> {
        let table_name = table.name;

        if self.tables.contains_key(table_name) {
            return Err(DbError::TableAlreadyExists(table_name.into()));
        }

        self.tables.insert(table_name, table);

        Ok(())
    }

    pub fn add_table_context(&mut self, table: Table<'a>) -> Result<()> {
        let table_name = table.name;
        let schema = table.record_batch.schema();
        let provider = MemTable::try_new(schema, vec![vec![table.record_batch]]).unwrap();

        self.ctx
            .register_table(table_name, Arc::new(provider))
            .unwrap();

        Ok(())
    }

    pub fn remove_table_context(&mut self, table: Table<'a>) -> Result<Arc<dyn TableProvider>> {
        let table_name = table.name;

        let provider = self.ctx.deregister_table(table_name).unwrap().unwrap();

        Ok(provider)
    }

    pub fn get_table(&self, name: &str) -> Result<Ref<'a, &str, Table>> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.into()))
    }

    pub fn get_mut_table(&self, name: &str) -> Result<RefMut<'a, &str, Table>> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.into()))
    }

    #[cfg(test)]
    pub fn print(&self) {
        for table in self.tables.iter() {
            table.value().print();
        }
    }
}

#[macro_export]
macro_rules! get_table {
    ( $self:ident, $name:ident ) => {
        $self
            .tables
            .get(&$name)
            .ok_or($crate::error::DbError::TableNotFound($name.into()))
    };
}

#[macro_export]
macro_rules! get_mut_table {
    ( $self:ident, $name:ident ) => {
        $self
            .tables
            .get_mut(&$name)
            .ok_or($crate::error::DbError::TableNotFound($name.into()))
    };
}

#[cfg(test)]
pub mod tests {

    use super::*;

    pub fn create_database<'a>() -> (Database<'a>, Table<'a>) {
        let mut database = Database::new("My DB");
        let table = Table::new("users");
        database.add_table(table.clone()).unwrap();

        (database, table)
    }

    #[test]
    fn test_database_and_table_creation() {
        let (mut database, table) = create_database();

        // expect an error when adding the same table
        assert_eq!(
            database.add_table(table.clone()),
            Err(DbError::TableAlreadyExists("users".into()))
        );

        let table_ref = database.tables.get("users").unwrap().clone();
        assert_eq!(table_ref, table);

        assert!(database.tables.get("non_existent_table").is_none());
    }
}
