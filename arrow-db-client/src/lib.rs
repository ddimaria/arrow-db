pub mod error;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use datafusion::arrow::datatypes::Schema;
// use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::util::pretty;
use tonic::codegen::StdError;
use tonic::transport::Channel;

use crate::error::{DbClientError, Result};

pub struct Client {
    inner: FlightServiceClient<Channel>,
}

impl Client {
    /// Create a new client
    pub async fn new<D>(endpoint: D) -> Result<Self>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError> + Send + Sync + 'static,
    {
        let client = FlightServiceClient::connect(endpoint)
            .await
            .map_err(|e| DbClientError::CreateClient(e.to_string()))?;

        Ok(Client { inner: client })
    }

    /// Get the schema of the RecordBatch
    pub async fn schema(&mut self) -> Result<Schema> {
        // Call get_schema to get the schema of the RecordBatch
        let request = tonic::Request::new(FlightDescriptor {
            r#type: DescriptorType::Cmd as i32, // Changed from Path to Cmd
            cmd: "get_schema".into(),           // Added command
            path: vec![],                       // Empty path since we're not using files
        });

        let schema_result = self
            .inner
            .get_schema(request)
            .await
            .map_err(|e| DbClientError::Schema(e.to_string()))?
            .into_inner();

        let schema =
            Schema::try_from(&schema_result).map_err(|e| DbClientError::Schema(e.to_string()))?;

        Ok(schema)
    }

    /// Execute a SQL query and receive results
    pub async fn query(&mut self, sql: &'static str) -> Result<Vec<RecordBatch>> {
        // Call do_get to execute a SQL query and receive results
        let request = tonic::Request::new(Ticket { ticket: sql.into() });

        let mut stream = self
            .inner
            .do_get(request)
            .await
            .map_err(|e| DbClientError::Query(e.to_string()))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        let flight_data = stream
            .message()
            .await
            .map_err(|e| DbClientError::Query(e.to_string()))?
            .unwrap();

        // convert FlightData to a stream
        let schema = Arc::new(
            Schema::try_from(&flight_data).map_err(|e| DbClientError::Query(e.to_string()))?,
        );
        println!("Schema: {schema:?}");

        // all the remaining stream messages should be dictionary and record batches
        let mut results = vec![];
        let dictionaries_by_field = HashMap::new();

        while let Some(flight_data) = stream
            .message()
            .await
            .map_err(|e| DbClientError::Query(e.to_string()))?
        {
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_field)
                    .map_err(|e| DbClientError::Query(e.to_string()))?;

            results.push(record_batch);
        }

        // print the results
        pretty::print_batches(&results).map_err(|e| DbClientError::Query(e.to_string()))?;

        Ok(results)
    }
}