# Kafka Wrapper for ID3 Parser CLI

This project is a Rust-based Kafka consumer-producer wrapper for an ID3 metadata parser CLI application. It listens to a Kafka topic for requests to parse `.mp3` files and responds with the extracted metadata via a different Kafka topic.

## CLI --> [rust-id3-manager](https://github.com/VillegasMich/rust-id3-manager)

## ✨ Features

- 🧾 **Consumes messages** from a Kafka topic (e.g., paths to `.mp3` files or base64-encoded files)
- ⚙️ **Parses metadata** using a CLI-based ID3 parser application
- 📤 **Produces parsed metadata** as JSON to a Kafka topic
- 🧵 Runs consumer and producer concurrently using `tokio`
- 🔧 Configurable via environment variables

---

## 🏗️ Architecture

```text
Kafka Topic (consume) ───▶ [Id3Consumer] ───▶ [ID3 Parser CLI] ───▶ [Id3Producer] ───▶ Kafka Topic (produce)

```
