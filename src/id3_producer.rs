use log::{error, info};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};

pub struct Id3Producer {
    pub broker: String,
    pub topic: String,
}

impl Id3Producer {
    pub fn new(broker: String, topic: String) -> Self {
        Self { broker, topic }
    }

    pub async fn produce(&self, message: String) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .create()
            .expect("Producer creation error");

        let topic = &self.topic;
        let record = FutureRecord::to(topic).payload(&message).key("key");

        match producer.send(record, Timeout::Never).await {
            Ok(delivery) => {
                println!("Message produced to topic: {}", &self.topic);
                info!("Sent: {:?}", delivery);
            }
            Err((e, _)) => error!("Error sending message: {:?}", e),
        }
    }
}
