"""
IoT Data Collector Service
Subscribes to MQTT, validates data, stores in PostgreSQL and raw files
"""
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_values
from pydantic import BaseModel, ValidationError, field_validator

# Configuration from environment
MQTT_HOST = os.getenv("MQTT_BROKER_HOST", "mqtt_broker")
MQTT_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/#")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "iot_data")
DB_USER = os.getenv("DB_USER", "iot_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "iot_password")
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", "/data/raw"))
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "100"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "5"))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SensorReading(BaseModel):
    """Validated sensor reading model"""
    sensor_id: str
    timestamp: datetime
    value: float
    unit: str
    
    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class DataCollector:
    def __init__(self):
        self.db_conn = None
        self.mqtt_client = None
        self.sensors_metadata = {}
        self.message_buffer = []
        self.buffer_size = BUFFER_SIZE
        self.last_flush = time.time()
        self.flush_interval = FLUSH_INTERVAL
        
        # Create raw data directory
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        
    def connect_db(self):
        """Establish PostgreSQL connection with retry logic"""
        max_retries = 10
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                self.db_conn.autocommit = False
                logger.info("Successfully connected to PostgreSQL")
                self.load_sensors_metadata()
                return
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"DB connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to database after {max_retries} attempts")
                    raise
    
    def load_sensors_metadata(self):
        """Load sensors from database into memory"""
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT sensor_id, location, sensor_type, unit, min_value, max_value FROM sensors")
        
        for row in cursor.fetchall():
            self.sensors_metadata[row[0]] = {
                'id': row[0],
                'location': row[1],
                'type': row[2],
                'unit': row[3],
                'range': {'min': row[4], 'max': row[5]}
            }
        
        logger.info(f"Loaded {len(self.sensors_metadata)} sensors from database")
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            client.subscribe(MQTT_TOPIC)
            logger.info(f"Subscribed to topic: {MQTT_TOPIC}")
        else:
            logger.error(f"Failed to connect to MQTT broker, code: {rc}")
    
    def on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            # Parse and validate message
            payload = json.loads(msg.payload.decode())
            reading = SensorReading(**payload)
            
            # Quality check
            quality_flag = self.check_data_quality(reading)
            
            # Add to buffer
            self.message_buffer.append((reading, quality_flag))
            
            # Store raw message to file
            self.store_raw_message(msg.topic, payload)
            
            # Flush buffer if full or time interval passed
            if len(self.message_buffer) >= self.buffer_size or \
               (time.time() - self.last_flush) >= self.flush_interval:
                self.flush_buffer()
            
        except ValidationError as e:
            logger.error(f"Invalid message format: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def check_data_quality(self, reading: SensorReading) -> str:
        """Check if reading is within expected range"""
        sensor_meta = self.sensors_metadata.get(reading.sensor_id)
        
        if not sensor_meta:
            return 'unknown_sensor'
        
        min_val = sensor_meta['range']['min']
        max_val = sensor_meta['range']['max']
        
        if reading.value < min_val or reading.value > max_val:
            return 'out_of_range'
        
        return 'valid'
    
    def store_raw_message(self, topic: str, payload: dict):
        """Store raw message to JSONL file"""
        date_str = datetime.now().strftime('%Y%m%d')
        raw_file = RAW_DATA_PATH / f"raw_{date_str}.jsonl"
        
        try:
            with open(raw_file, 'a') as f:
                json.dump({
                    'topic': topic,
                    'timestamp': datetime.now().isoformat(),
                    'payload': payload
                }, f)
                f.write('\n')
        except Exception as e:
            logger.error(f"Failed to write raw message: {e}")
    
    def flush_buffer(self):
        """Batch insert buffered messages to database"""
        if not self.message_buffer:
            return
        
        try:
            cursor = self.db_conn.cursor()
            
            # Prepare batch insert
            values = [
                (
                    reading.sensor_id,
                    reading.timestamp,
                    reading.value,
                    quality_flag
                )
                for reading, quality_flag in self.message_buffer
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO measurements (sensor_id, timestamp, value, quality_flag)
                VALUES %s
                """,
                values
            )
            
            self.db_conn.commit()
            logger.info(f"Inserted {len(values)} measurements")
            
            self.message_buffer.clear()
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            self.db_conn.rollback()
    
    def setup_mqtt(self):
        """Setup MQTT client"""
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        
        # Connect with retry
        max_retries = 10
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
                logger.info("MQTT client setup complete")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"MQTT connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay)
                else:
                    raise
    
    def run(self):
        """Main execution loop"""
        logger.info("Starting IoT Data Collector")
        
        # Connect to database
        self.connect_db()
        
        # Setup MQTT
        self.setup_mqtt()
        
        # Start MQTT loop
        self.mqtt_client.loop_forever()


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()