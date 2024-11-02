use std::sync::Arc;

use datafusion::{catalog::TableProvider, datasource::MemTable, prelude::DataFrame};

use crate::{
    database::Database,
    error::{DbError, Result},
    table::Table,
};

impl<'a> Database<'a> {
    pub fn add_table_context(&self, table: Table<'a>) -> Result<()> {
        let table_name = table.name;
        let schema = table.record_batch.schema();
        let provider = MemTable::try_new(schema, vec![vec![table.record_batch]]).unwrap();

        self.ctx
            .register_table(table_name, Arc::new(provider))
            .unwrap();

        Ok(())
    }

    pub fn add_all_table_contexts(&self) -> Result<()> {
        for table in self.tables.iter() {
            // TODO(ddimaria): remove this clone
            self.add_table_context(table.value().to_owned())?;
        }

        Ok(())
    }

    pub fn remove_table_context(&mut self, table: Table<'a>) -> Result<Arc<dyn TableProvider>> {
        let table_name = table.name;

        let provider = self.ctx.deregister_table(table_name).unwrap().unwrap();

        Ok(provider)
    }

    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Query(sql.into(), e.to_string()))?;

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
    use crate::database::tests::{create_database, seed_database};

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

        database
            .test_query("insert into user_role values (5, 'manager')")
            .await;

        database
            .test_query("select * from users inner join user_role on users.id = user_role.user_id ")
            .await;

        database
            .test_query(
                "select * from users inner join user_role on users.id = user_role.user_id 
                where id > 1 
                order by name desc",
            )
            .await;

        // let sql_df = database
        //     .ctx
        //     .sql("update users set name = 'Eve2' where id = 5")
        //     .await
        //     .unwrap();
        // sql_df.show().await.unwrap();

        // let batch = database.remove_table_context(table).unwrap();
    }
}
