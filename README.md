# IoT Data Engineer Challenge

This project implements a full IoT data pipeline for collecting, storing, and querying sensor data.

## Challenge Implementation Status

**Step 1**: Collect and store IoT data in raw format to low-cost long-term storage  
**Step 2**: Make data available for BI specialists to query historical data (near real-time)  
**Step 3**: Optional features - Database schema prepared but not implemented (resampling, data quality indicators)

## Project Overview

### Architecture

```bash
Generator -> MQTT Broker -> Collector -> PostgreSQL + Raw Files
```

- **Generator**: Simulates 5 IoT sensors and publishes data via MQTT (1 reading/second per sensor)
- **MQTT Broker**: Eclipse Mosquitto - manages connections and message routing
- **Collector**: Subscribes to MQTT topics, validates data, and stores in:
  - PostgreSQL (structured data for queries)
  - Raw JSONL files (long-term storage, one file per day)
- **PostgreSQL**: Stores structured measurements with indexes for efficient querying
- **Raw Files**: JSONL format in `./data/raw/` directory (backup and data lineage)

### Features

- Batch processing: Reduces database load (buffers 100 messages or 5 seconds)
- Data validation: Pydantic models ensure data quality
- Quality flags: Tracks valid, out_of_range, and unknown_sensor readings
- Near real-time: Data available for querying within 5 seconds
- Query support: 18 example queries provided for BI analysts and data scientists

## Setup & Run

### 1. Build and start the environment:

```bash
docker compose up -d
```

### 2. Verify containers are running:

```bash
docker compose ps
```

### 3. Check sensor measurements in PostgreSQL:

```bash
docker exec -it postgres psql -U iot_user -d iot_data -c "SELECT sensor_id, COUNT(*) FROM measurements GROUP BY sensor_id;"
```

### 4. Access the database

**Via psql:**
```bash
docker exec -it postgres psql -U iot_user -d iot_data
```

**Via pgAdmin (optional):**
```bash
docker compose --profile admin up -d pgadmin
```
Then access at http://localhost:5050 (admin@axpo.com / admin)

### 5. Query the data

See `example_queries.sql` for 18 ready-to-use queries including:
- Latest readings per sensor
- Hourly aggregations
- Data quality metrics
- Anomaly detection
- Sensor correlations
- And more...

Example:
```bash
docker exec -it postgres psql -U iot_user -d iot_data -f /path/to/example_queries.sql
```

### 6. Access raw data files

Raw JSONL files are stored in `./data/raw/` directory:
```bash
ls -lh ./data/raw/
cat ./data/raw/raw_20240101.jsonl  # Example date
```

## Design Decisions

- **Batch insertion**: Reduces database transactions, improves performance (100 messages or 5s interval)
- **Raw + structured storage**: Allows data validation, reprocessing, and audit trails
- **Collector service**: Decouples ingestion from generation, enables independent scaling
- **Dockerized environment**: Ensures reproducibility and easy deployment
- **Indexed database**: Optimized for time-series queries (sensor_id + timestamp)
- **Quality flags**: Enables filtering of invalid data in queries

## Database Schema

- **sensors**: Sensor metadata (location, type, unit, min/max values)
- **measurements**: Time-series data with quality flags
- **Views**: `latest_readings`, `hourly_aggregates` for common queries
- **Indexes**: Optimized for sensor_id + timestamp queries

## Results

After running the pipeline, you should see data like:

| sensor_id | count |
|-----------|-------|
| Sensor 1  | 120   |
| Sensor 2  | 120   |
| Sensor 3  | 120   |
| Sensor 4  | 119   |
| Sensor 5  | 119   |

## Notes

- Step 3 (optional features) was not implemented to focus on core requirements and solution design
- Database schema includes tables for resampling (`measurements_1min`) and data quality tracking, ready for future implementation

