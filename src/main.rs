use std::env;

use id3_consumer::Id3Consumer;
use id3_parser::Id3ParserCaller;

pub mod id3_consumer;
pub mod id3_parser;
pub mod id3_producer;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv::dotenv().ok();

    let broker = env::var("BROKER").expect("No BROKER variable found in the .env");
    let consume_topic =
        env::var("CONSUME_TOPIC").expect("No CONSUME_TOPIC variable found in the .env");
    let path_parser =
        env::var("PATH_TO_ID3_PARSER").expect("No PATH_TO_ID3_PARSER variable found in the .env");

    let id3_parser_caller = Id3ParserCaller::new(path_parser);
    let id3_consumer = Id3Consumer::new(broker.clone(), consume_topic, id3_parser_caller);

    println!("Rust kafka producer-consumer started...");

    id3_consumer.consume().await;
}
