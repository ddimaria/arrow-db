use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;

// use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{/*FlightDescriptor,*/ Ticket};
use datafusion::arrow::util::pretty;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Flight client
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    // Call get_schema to get the schema of the RecordBatch
    // let request = tonic::Request::new(FlightDescriptor {
    //     r#type: flight_descriptor::DescriptorType::Cmd as i32, // Changed from Path to Cmd
    //     cmd: "get_schema".into(),                              // Added command
    //     path: vec![], // Empty path since we're not using files
    // });

    // let schema_result = client.get_schema(request).await?.into_inner();
    // let schema = Schema::try_from(&schema_result)?;
    // println!("Schema: {schema:?}");

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket {
        ticket: "SELECT * FROM users".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    println!("Schema: {schema:?}");

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    let dictionaries_by_field = HashMap::new();
    while let Some(flight_data) = stream.message().await? {
        let record_batch =
            flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_field)?;
        results.push(record_batch);
    }

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
