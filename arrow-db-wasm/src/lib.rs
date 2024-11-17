mod utils;

use arrow_db_core::Database;
use bytes::Bytes;
use chrono::Utc;
use serde_wasm_bindgen;
use utils::set_panic_hook;
use utils::to_serializable;
use utils::SerializableRecordBatch;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures;

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
    pub fn get_tables(&self) -> Vec<String> {
        self.database
            .tables
            .iter()
            .map(|k| k.key().to_string())
            .collect()
    }

    #[wasm_bindgen]
    pub fn get_schemas(&self) -> Vec<String> {
        self.database
            .tables
            .iter()
            .map(|k| {
                format!(
                    "{}: {}",
                    k.key(),
                    k.value().record_batch.schema().to_string()
                )
            })
            .collect()
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
