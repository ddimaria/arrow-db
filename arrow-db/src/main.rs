use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_schema::Schema;
use std::sync::Arc;

use arrow_db_core::Database;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use arrow_flight::{PollInfo, SchemaAsIpc};
use datafusion::arrow::error::ArrowError;
use datafusion::prelude::*;
use futures::stream::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightServiceImpl {
    pub state: Arc<SessionContext>,
}

impl FlightServiceImpl {
    pub async fn new() -> Result<Self, Status> {
        Ok(Self {
            state: Arc::new(Self::new_context().await?),
        })
    }

    async fn new_context() -> Result<SessionContext, Status> {
        let database: Database = Database::new_from_disk("MyDb").await.unwrap();
        database.add_all_table_contexts().unwrap();

        Ok(database.ctx)
    }

    pub async fn get_schema(&self) -> Result<Schema, Status> {
        let schema: Schema = self
            .state
            .table("mytable")
            .await
            .map_err(to_tonic_err)?
            .schema()
            .into();

        Ok(schema)
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();
        println!("get_schema: {:?}", request);

        let schema = self.get_schema().await?;
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result: SchemaResult = schema_ipc
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        println!("do_get: {:?}", ticket);
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                println!("do_get: {sql}");

                let ctx = Arc::clone(&self.state);

                // create the DataFrame
                let df = ctx.sql(sql).await.map_err(to_tonic_err)?;

                // execute the query
                let schema = df.schema().clone().into();
                let results = df.collect().await.map_err(to_tonic_err)?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }

                // add an initial FlightData message that sends schema
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let schema_flight_data = SchemaAsIpc::new(&schema, &options);

                let mut flights = vec![FlightData::from(schema_flight_data)];

                let encoder = IpcDataGenerator::default();
                let mut tracker = DictionaryTracker::new(false);

                for batch in &results {
                    let (flight_dictionaries, flight_batch) = encoder
                        .encoded_batch(batch, &mut tracker, &options)
                        .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

                    flights.extend(flight_dictionaries.into_iter().map(Into::into));
                    flights.push(flight_batch.into());
                }

                let output = futures::stream::iter(flights.into_iter().map(Ok));
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_tonic_err(e: datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{e:?}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightServiceImpl::new().await?;
    let svc = FlightServiceServer::new(service);

    println!("Listening on {addr:?}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
