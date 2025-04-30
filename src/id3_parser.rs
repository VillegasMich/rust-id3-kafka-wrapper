use std::{
    io,
    process::{Command, Stdio},
};

use log::{error, info, warn};
use rdkafka::{message::BorrowedMessage, Message};
use regex::Regex;
use serde_json::Value;

pub struct Id3ParserCaller {
    pub path_parser: String,
}

impl Id3ParserCaller {
    pub fn new(path_to_parser: String) -> Id3ParserCaller {
        Id3ParserCaller {
            path_parser: path_to_parser,
        }
    }

    pub fn call_id3_parser(&self, msg: &BorrowedMessage) -> io::Result<()> {
        let start = std::time::Instant::now();

        let payload = msg.payload_view::<str>().unwrap_or(Ok("")).unwrap_or("");
        let key = msg.key_view::<str>().unwrap_or(Ok("")).unwrap_or("");
        info!("Received message: key='{}', payload='{}'", key, payload);

        let path_parser = &self.path_parser;
        let path_expanded = shellexpand::full(&path_parser).expect("Bad absolute path for parser");

        let music_path = shellexpand::full(payload)
            .expect("Path expansion failed")
            .into_owned();

        let child = Command::new(path_expanded.to_string())
            .arg("-f")
            .arg(&music_path)
            .arg("-s")
            .arg("-j")
            .stdout(Stdio::piped())
            .spawn()
            .expect("Id3 parser tool did not worked correctly");

        let output = child
            .wait_with_output()
            .expect("Can not extract the output");

        if !output.status.success() {
            error!(
                "Parser failed with: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Err(io::Error::new(io::ErrorKind::Other, "Parser failed"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        let re = Regex::new(r"\{[\s\S]*\}").unwrap();
        let json_text = re
            .find(&stdout)
            .map(|m| m.as_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No JSON block found"))?;

        let json_value: Value = serde_json::from_str(json_text).expect("Error parsing to JSON");
        println!(
            "Parsed JSON:\n{}",
            serde_json::to_string_pretty(&json_value)
                .expect("Something happened with the pretty print")
        );

        let duration = start.elapsed();
        info!("ID3 parsing took: {:.2?}", duration);

        if duration > std::time::Duration::from_secs(5) {
            warn!("Slow ID3 parsing detected");
        }

        Ok(())
    }
}
