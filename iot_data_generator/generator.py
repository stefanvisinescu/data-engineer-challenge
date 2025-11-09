import json
import random
import time
import logging
from pathlib import Path
import paho.mqtt.client as mqtt

class Sensor:
    def __init__(self, sensor_id, location, sensor_type, unit, min_value, max_value):
        self.id = sensor_id
        self.location = location
        self.type = sensor_type
        self.unit = unit
        self.min_value = min_value
        self.max_value = max_value

class Generator:
    def __init__(self, mqtt_host="mqtt_broker", mqtt_port=1883, publish_interval=1):
        self.logger = logging.getLogger("generator")
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        self.sensors = []
        self._load_sensors()

        self.publish_interval = publish_interval
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.client = mqtt.Client()
        self._connect_mqtt()

    def _load_sensors(self):
        """Load sensor definitions from sensors.json"""
        sensors_file = Path(__file__).parent / "sensors.json"

        if not sensors_file.exists():
            self.logger.error(f"sensors.json not found at {sensors_file}")
            raise FileNotFoundError(f"sensors.json not found at {sensors_file}")

        with open(sensors_file, 'r') as f:
            data = json.load(f)

        for sensor_data in data.get('sensors', []):
            sensor = Sensor(
                sensor_id=sensor_data['id'],
                location=sensor_data['location'],
                sensor_type=sensor_data['type'],
                unit=sensor_data['unit'],
                min_value=sensor_data['range']['min'],
                max_value=sensor_data['range']['max']
            )
            self.sensors.append(sensor)

        self.logger.info(f"Loaded {len(self.sensors)} sensors from {sensors_file}")

    def _connect_mqtt(self):
        """Connect to MQTT broker"""
        self.client.connect(self.mqtt_host, self.mqtt_port)
        self.logger.info(f"Successfully connected to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")

    def _generate_value(self, sensor):
        """Generate a random value within sensor range"""
        return round(random.uniform(sensor.min_value, sensor.max_value), 2)

    def _publish_sensor_data(self, sensor):
        """Publish a single sensor's data to MQTT"""
        topic = f"sensors/{sensor.id.replace(' ', '_')}"
        payload = {
            "sensor_id": sensor.id,
            "type": sensor.type,
            "value": self._generate_value(sensor),
            "unit": sensor.unit,
            "location": sensor.location,
            "timestamp": int(time.time())
        }
        self.client.publish(topic, json.dumps(payload))
        self.logger.info(f"Published to {topic}: {payload}")

    def run(self):
        """Start publishing sensor data in a loop"""
        self.logger.info(f"Starting IoT Data Generator - Publishing every {self.publish_interval}s")
        try:
            while True:
                for sensor in self.sensors:
                    self._publish_sensor_data(sensor)
                time.sleep(self.publish_interval)
        except KeyboardInterrupt:
            self.logger.info("Stopping generator...")

