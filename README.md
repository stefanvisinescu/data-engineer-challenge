# IoT Data Engineer Challenge

This project implements a full IoT data pipeline for collecting, storing, and querying sensor data. 

## Project Overview

- Collects data from 5 simulated IoT sensors.
- Publishes data via MQTT.
- Data is stored in PostgreSQL and raw files for further analysis.
- Supports batch processing and near real-time ingestion.

## Setup & Run

1. Build and start the environment:

```bash
docker compose up -d
```
2. Verify containers are running:
```bash
docker compose ps
```
3.Check sensor measurements in PostgreSQL:
```bash
docker exec -it postgres psql -U iot_user -d iot_data -c "SELECT sensor_id, COUNT(*) FROM measurements GROUP BY sensor_id;"
```
## Project Overview

```bash
Generator -> MQTT Broker -> Collector -> PostgreSQL + Raw Files
```
- Generator: Simulates sensors and publishes data via MQTT.

- MQTT Broker: Manages connections and topics.

- Collector: Subscribes to MQTT topics and stores data in PostgreSQL + raw JSON files.

- PostgreSQL: Stores structured measurements for queries and analysis.

- Raw files: Backup and lineage of all messages.

## Design Decisions
- Batch insertion: Reduces database transactions, improves performance.

- Raw + structured storage: Allows data validation and downstream processing.

- Collector service: Decouples ingestion from generation.

- Dockerized environment: Ensures reproducibility.

## Results

|sensor_id | count|
|--------|-------|
|Sensor 1  | 120|
|Sensor 2  | 120|
|Sensor 3  | 120|
|Sensor 4  | 119|
|Sensor 5  | 119|

