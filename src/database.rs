use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};

use crate::{
    error::{DbError, Result},
    table::Table,
};

#[derive(Debug, Clone)]
pub struct Database<'a> {
    pub name: &'a str,
    pub tables: DashMap<&'a str, Table<'a>>,
}

impl<'a> Database<'a> {
    pub fn new(name: &'a str) -> Database<'a> {
        Database {
            name,
            tables: DashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table<'a>) -> Result<()> {
        if self.tables.contains_key(table.name) {
            return Err(DbError::TableAlreadyExists(table.name.into()));
        }

        self.tables.insert(table.name, table);

        Ok(())
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
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;

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

    #[test]
    fn test_database_columns() {
        let (database, table) = create_database();

        let name = table.name;
        get_mut_table!(database, name)
            .unwrap()
            .add_column(0, "id", DataType::Int32)
            .unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![1, 2]).into())
            .unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .append_column_data::<Int32Array>(0, Int32Array::from(vec![3]).into())
            .unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .insert_column_data_at::<Int32Array>(0, 2, Int32Array::from(vec![4]).into())
            .unwrap();

        let add_column =
            get_mut_table!(database, name)
                .unwrap()
                .add_column(1, "name", DataType::Utf8);
        add_column.unwrap();

        get_mut_table!(database, name)
            .unwrap()
            .append_column_data::<StringArray>(
                1,
                StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]).into(),
            )
            .unwrap();

        database.print();
    }
}
