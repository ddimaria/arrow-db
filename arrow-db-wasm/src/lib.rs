mod utils;

use arrow_db_core::Database;
use bytes::Bytes;
use chrono::Utc;
use utils::set_panic_hook;
use utils::to_serializable;
use utils::{
    PaginatedResult, PaginationMetadata, SchemaField, SerializableRecordBatch, TableSchema,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub(crate) fn log(s: &str);
}

#[wasm_bindgen]
pub struct ArrowDbWasm {
    database: Database<'static>,
}

#[wasm_bindgen]
impl ArrowDbWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(name: String) -> ArrowDbWasm {
        set_panic_hook();

        let name = Box::new(name.to_string());
        let database = Database::new(Box::leak(name.clone())).unwrap();

        ArrowDbWasm { database }
    }

    #[wasm_bindgen]
    pub fn read_file(&mut self, table_name: String, file_bytes: Vec<u8>) -> Result<(), JsValue> {
        set_panic_hook();

        let total = Utc::now();
        let bytes = Bytes::from(file_bytes);

        self.database
            .load_table_bytes(table_name.to_owned(), bytes)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.database.add_table_context(&table_name).unwrap();

        let elapsed = Utc::now() - total;
        log(&format!("Total Time in read_file(): {:.2?}", elapsed));

        Ok(())
    }

    #[wasm_bindgen]
    pub async fn query(&self, sql: String) -> Result<JsValue, JsValue> {
        set_panic_hook();

        let total = Utc::now();
        let now = Utc::now();

        let session_id = self.database.ctx.session_id();
        log(&format!("Session ID: {:?}", session_id));

        let elapsed = Utc::now() - now;
        let now = Utc::now();
        log(&format!("Added Table Contexts in {:.2?}", elapsed));

        let data_frame = self
            .database
            .query(&sql)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let elapsed = Utc::now() - now;
        let now = Utc::now();
        log(&format!("Queried file {:.2?}", elapsed));

        let headers = data_frame.schema().clone().strip_qualifiers().field_names();

        let elapsed = Utc::now() - now;
        let now = Utc::now();
        log(&format!("Read in headers in {:.2?}", elapsed));

        let record_batches = data_frame.collect().await.map_err(|e| e.to_string())?;

        let elapsed = Utc::now() - now;
        let now = Utc::now();
        log(&format!("Collected record batches in {:.2?}", elapsed));

        let serializable_record_batches = record_batches
            .iter()
            .map(|batch| to_serializable(&headers, batch))
            .collect::<Vec<SerializableRecordBatch>>();

        let elapsed = Utc::now() - now;
        log(&format!("Serialized record batches in {:.2?}", elapsed));

        let elapsed = Utc::now() - total;
        log(&format!("Total Time: {:.2?}", elapsed));

        Ok(serde_wasm_bindgen::to_value(&serializable_record_batches).unwrap())
    }

    #[wasm_bindgen]
    pub async fn query_paginated(
        &self,
        sql: String,
        page: usize,
        page_size: usize,
        include_total_count: bool,
    ) -> Result<JsValue, JsValue> {
        set_panic_hook();

        let total = Utc::now();
        log(&format!(
            "Starting paginated query - page: {}, page_size: {}, count: {}",
            page, page_size, include_total_count
        ));

        let (data_frame, pagination_info) = self
            .database
            .query_paginated(&sql, page, page_size, include_total_count)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let elapsed = Utc::now() - total;
        log(&format!("Paginated query executed in {:.2?}", elapsed));

        let headers = data_frame.schema().clone().strip_qualifiers().field_names();
        let record_batches = data_frame.collect().await.map_err(|e| e.to_string())?;

        let serializable_record_batches = record_batches
            .iter()
            .map(|batch| to_serializable(&headers, batch))
            .collect::<Vec<SerializableRecordBatch>>();

        let pagination_metadata = PaginationMetadata {
            page: pagination_info.page,
            page_size: pagination_info.page_size,
            rows_in_page: pagination_info.rows_in_page,
            total_rows: pagination_info.total_rows,
            total_pages: pagination_info.total_pages,
            has_next_page: pagination_info.has_next_page,
            has_previous_page: pagination_info.has_previous_page,
        };

        let result = PaginatedResult {
            data: serializable_record_batches,
            pagination: pagination_metadata,
        };

        let elapsed = Utc::now() - total;
        log(&format!("Total paginated query time: {:.2?}", elapsed));

        Ok(serde_wasm_bindgen::to_value(&result).unwrap())
    }

    #[wasm_bindgen]
    pub fn get_tables(&self) -> Vec<String> {
        self.database
            .tables
            .iter()
            .map(|k| k.key().to_string())
            .collect()
    }

    #[wasm_bindgen]
    pub fn get_schemas(&self) -> Result<JsValue, JsValue> {
        let schemas: Vec<TableSchema> = self
            .database
            .tables
            .iter()
            .map(|table_entry| {
                let table_name = table_entry.key().to_string();
                let schema = table_entry.value().record_batch.schema();

                let fields: Vec<SchemaField> = schema
                    .fields()
                    .iter()
                    .map(|field| SchemaField {
                        name: field.name().clone(),
                        data_type: field.data_type().to_string(),
                        nullable: field.is_nullable(),
                    })
                    .collect();

                TableSchema { table_name, fields }
            })
            .collect();

        serde_wasm_bindgen::to_value(&schemas).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen]
    pub fn remove_table(&mut self, table_name: String) -> Result<(), JsValue> {
        set_panic_hook();

        // Log current tables for debugging
        let current_tables: Vec<String> = self
            .database
            .tables
            .iter()
            .map(|k| k.key().to_string())
            .collect();
        log(&format!("Current tables: {:?}", current_tables));
        log(&format!("Attempting to remove table: {}", table_name));

        self.database
            .remove_table(&table_name)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        log(&format!("Successfully removed table: {}", table_name));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_read_file() {
        // let result = read_file(vec![]).await;
        // assert!(result.is_ok());
    }
}
