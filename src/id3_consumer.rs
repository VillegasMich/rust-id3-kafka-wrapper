use log::error;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig,
};

use tokio_stream::StreamExt;

use crate::id3_parser::Id3ParserCaller;

pub struct Id3Consumer {
    pub broker: String,
    pub topic: String,
    pub parser: Id3ParserCaller,
}

impl Id3Consumer {
    pub fn new(broker: String, topic: String, parser: Id3ParserCaller) -> Self {
        Self {
            broker,
            topic,
            parser,
        }
    }

    pub async fn consume(&self) {
        let broker = &self.broker;
        let topic = &self.topic;
        println!("Consuming messages from topic {}", topic);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "my-group")
            .set("bootstrap.servers", broker)
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Consumer creation failed");
        consumer
            .subscribe(&[topic])
            .expect("Can't subscribe to topic");

        let mut message_stream = consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(message) => {
                    let _ = self.parser.call_id3_parser(&message);
                }
                Err(e) => error!("Kafka error: {}", e),
            }
        }
    }
}
