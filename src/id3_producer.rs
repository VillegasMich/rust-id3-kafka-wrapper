use log::{error, info};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use std::time::Duration;

pub struct Id3Producer {
    pub broker: String,
    pub topic: String,
}

impl Id3Producer {
    pub fn new(broker: String, topic: String) -> Self {
        Self { broker, topic }
    }

    pub async fn produce(&self) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .create()
            .expect("Producer creation error");

        loop {
            let topic = &self.topic;
            let record = FutureRecord::to(topic)
                .payload("~/Music/hiræth, Willix - Intranet Crush [NCS Release].mp3")
                .key("key");

            match producer.send(record, Timeout::Never).await {
                Ok(delivery) => {
                    println!("Message produced");
                    info!("Sent: {:?}", delivery);
                }
                Err((e, _)) => error!("Error sending message: {:?}", e),
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }
}
