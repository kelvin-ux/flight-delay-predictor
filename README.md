# Flight Delay Prediction System

Real-time flight delay prediction system using Apache Kafka for message streaming and PySpark ML for predictions.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Kafka Topics](#kafka-topics)
- [Troubleshooting](#troubleshooting)

## Overview

This system provides flight delay predictions using two types of machine learning models:

| Model Type | Description | Output |
|------------|-------------|--------|
| Regression | Predicts expected delay in minutes | Continuous value (e.g., +37 min) |
| Classification | Predicts probability of significant delays | Percentage for >30min and >60min delays |

The system supports two operational modes:
- Model A: Planning mode (without departure delay data)
- Model B: Live operations mode (with real-time departure delay)

## Architecture

```
                                    KAFKA CLUSTER
                                         
Frontend ──> Backend ──> input-regression ──────> Consumer Thread ──> output-regression ────>
                    └──> input-classification ──> Consumer Thread ──> output-classification ─┤
                                                                                             │
                    <───────────────────── Backend aggregates results <──────────────────────┘
                                                     │
Frontend <───────────────────────────────────────────┘
```

Data Flow:
1. User submits flight parameters via frontend
2. Backend sends data to both input topics simultaneously
3. Consumer threads process requests using PySpark ML models
4. Results are published to output topics
5. Backend waits for responses from both output topics
6. Aggregated results are returned to frontend

## Prerequisites

- Python 3.9+
- Apache Kafka 2.8+
- Apache Spark 3.x
- Java 8 or 11

### Python Dependencies

```
flask
flask-cors
kafka-python
pyspark
```

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd flight-prediction
```

### 2. Install Python Dependencies

```bash
pip install flask flask-cors kafka-python pyspark
```

### 3. Configure Java Environment

Edit `backend.py` and update the JAVA_HOME path:

```bash
os.environ["JAVA_HOME"] = "/path/to/your/java/home"
```

### 4. Prepare ML Models

Ensure the following model directories exist in the project root:
- `model_a_gbt/` - Regression model for planning mode
- `model_b_lr/` - Regression model for live mode
- `model_classification_30/` - Classification model for >30min delays
- `model_classification_60/` - Classification model for >60min delays
- `aggregated_stats.pkl` - Precomputed statistics file

## Configuration

### Kafka Configuration

Default settings in `backend.py`:

```python
KAFKA_BROKER = 'localhost:9092'
RESPONSE_TIMEOUT = 10.0  # seconds
```

### Topic Configuration

| Topic | Direction | Purpose |
|-------|-----------|---------|
| input-regression | Inbound | Regression model requests |
| input-classification | Inbound | Classification model requests |
| output-regression | Outbound | Regression model results |
| output-classification | Outbound | Classification model results |

## Usage

### Step 1: Start Zookeeper

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

### Step 2: Start Kafka Server

Open a new terminal:

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

### Step 3: Create Topics

Run once during initial setup:

```bash
~/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic input-regression \
    --partitions 3 \
    --replication-factor 1

~/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic input-classification \
    --partitions 3 \
    --replication-factor 1

~/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic output-regression \
    --partitions 3 \
    --replication-factor 1

~/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic output-classification \
    --partitions 3 \
    --replication-factor 1
```

Verify topics were created:

```bash
~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 4: Start Backend Server

```bash
python backend.py
```

Expected output:

```
Regression models loaded
Classification models loaded
Stats loaded
Kafka Producer connected
Consumer started: input-regression
Consumer started: input-classification
============================================================
IN: input-regression, input-classification
OUT: output-regression, output-classification
Timeout: 10.0s
============================================================
```

### Step 5: Open Frontend

Open `index.html` in a web browser.

## API Reference

### POST /predict

Submit flight data for prediction using both models.

Request Body:
```json
{
    "model_type": "A",
    "UniqueCarrier": "AA",
    "Origin": "ORD",
    "Dest": "LAX",
    "Distance": 1745,
    "DepHour": 14,
    "DayOfWeek": 5,
    "Month": 12,
    "Day": 15,
    "CRSElapsedTime": 240,
    "CRSArrHour": 18
}
```

Response:
```json
{
    "status": "success",
    "request_id": "uuid",
    "regression": {
        "predicted_delay_minutes": 37.5,
        "model_type": "A",
        "status": "success"
    },
    "classification": {
        "probability_over_30min": 42.3,
        "probability_over_60min": 15.8,
        "status": "success"
    }
}
```

### POST /predict-regression

Submit flight data for regression prediction only.

### POST /predict-classification

Submit flight data for classification prediction only.

### GET /distance

Get distance between two airports.

Parameters:
- `origin` - Origin airport code
- `dest` - Destination airport code

### GET /health

Check system health status.

## Kafka Topics

### Monitoring Topics

View messages in real-time:

```bash
# Input regression topic
~/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic input-regression \
    --from-beginning

# Output classification topic
~/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic output-classification \
    --from-beginning
```

### Topic Management

Delete a topic:
```bash
~/kafka/bin/kafka-topics.sh --delete \
    --bootstrap-server localhost:9092 \
    --topic <topic-name>
```

Describe topic configuration:
```bash
~/kafka/bin/kafka-topics.sh --describe \
    --bootstrap-server localhost:9092 \
    --topic <topic-name>
```

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Connection refused on port 9092 | Kafka not running | Start Kafka server |
| Connection refused on port 2181 | Zookeeper not running | Start Zookeeper first |
| Topic already exists | Topic was previously created | Safe to ignore or delete and recreate |
| Timeout waiting for prediction | Model processing slow or consumer not running | Check backend logs, increase RESPONSE_TIMEOUT |
| Models not loaded | Missing model files | Verify model directories exist |

### Checking Service Status

Verify Zookeeper is running:
```bash
echo ruok | nc localhost 2181
```

List active consumer groups:
```bash
~/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --list
```

### Log Locations

- Kafka logs: `~/kafka/logs/`
- Zookeeper logs: `~/kafka/logs/`
- Backend logs: Console output

### Resetting State

Stop all services and clear data:
```bash
# Stop Kafka and Zookeeper first, then:
rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper
```

## License

This project is provided for educational purposes.