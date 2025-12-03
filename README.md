# Kafka Setup Guide

This guide provides step-by-step instructions for setting up and using Apache Kafka for flight predictions.

## Prerequisites

- Apache Kafka installed in `~/kafka/` directory
- Java 8+ installed

## Getting Started

### 1. Start Zookeeper

Zookeeper must be running before starting Kafka.

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

### 2. Start Kafka Server

In a new terminal window, start the Kafka broker:

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

### 3. Create Topic

Create a topic named `flight-predictions` with 3 partitions:

```bash
~/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic flight-predictions \
  --partitions 3 \
  --replication-factor 1
```

### 4. Start Producer

Open a new terminal to start sending messages:

```bash
~/kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic flight-predictions
```

Type messages and press Enter to send them to the topic.

### 5. Start Consumer

In another terminal, start consuming messages:

```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic flight-predictions \
  --from-beginning
```

This will display all messages from the beginning of the topic.

## Notes

- Keep Zookeeper and Kafka server running in separate terminal windows
- The producer and consumer can run simultaneously
- Use `Ctrl+C` to stop any running process

## Troubleshooting

- If the topic already exists, you'll get an error when trying to create it again
- Ensure ports 2181 (Zookeeper) and 9092 (Kafka) are not in use by other applications
- Check logs in `~/kafka/logs/` if services fail to start