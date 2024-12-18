extern crate arrow_db_client;

use arrow::util::pretty;
use arrow_db_client::Client;
use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    query: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut client = Client::new("http://localhost:50051").await.unwrap();
    let results = client.query(&args.query).await.unwrap();

    // print the results
    println!("\nQuery: {}", args.query);
    pretty::print_batches(&results).unwrap();
}
