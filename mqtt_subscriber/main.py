import os
import json
import psycopg2
import paho.mqtt.client as mqtt

# Config from environment variables
MQTT_HOST = os.environ.get("MQTT_HOST", "mqtt_broker")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "db")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "iot")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Connect to Postgres
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cursor = conn.cursor()

# Ensure table exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS measurements (
    sensor_id TEXT,
    type TEXT,
    value REAL,
    unit TEXT,
    location TEXT,
    timestamp TIMESTAMP
)
""")
conn.commit()

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe("sensors/#")  # subscribe to all sensors

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    cursor.execute(
        "INSERT INTO measurements (sensor_id, type, value, unit, location, timestamp) VALUES (%s,%s,%s,%s,%s,TO_TIMESTAMP(%s))",
        (data['sensor_id'], data['type'], data['value'], data['unit'], data['location'], data['timestamp'])
    )
    conn.commit()
    print(f"Saved: {data}")

# MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()
