"""
Sensor class for generating realistic sensor readings
"""
import random
from datetime import datetime
from typing import Dict


class Sensor:
    """Represents an IoT sensor that generates readings"""
    
    def __init__(
        self,
        sensor_id: str,
        location: str,
        sensor_type: str,
        unit: str,
        min_value: float,
        max_value: float
    ):
        """
        Initialize sensor
        
        Args:
            sensor_id: Unique identifier for the sensor
            location: Physical location of the sensor
            sensor_type: Type of sensor (e.g., temperature, humidity)
            unit: Unit of measurement (e.g., Celsius, %)
            min_value: Minimum possible value
            max_value: Maximum possible value
        """
        self.sensor_id = sensor_id
        self.location = location
        self.sensor_type = sensor_type
        self.unit = unit
        self.min_value = min_value
        self.max_value = max_value
        
        # Initialize with a random value in range
        self.current_value = random.uniform(min_value, max_value)
    
    def generate_reading(self) -> Dict:
        """
        Generate a sensor reading with realistic variation
        
        Returns:
            Dictionary with sensor reading data
        """
        # Add small random variation to simulate realistic changes
        variation = random.uniform(-0.5, 0.5)
        self.current_value += variation
        
        # Keep value within bounds
        self.current_value = max(self.min_value, min(self.max_value, self.current_value))
        
        # Create reading
        reading = {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "value": round(self.current_value, 2),
            "unit": self.unit
        }
        
        return reading
    
    def __repr__(self) -> str:
        return f"Sensor({self.sensor_id}, {self.location}, {self.sensor_type})"