"""
Settings configuration for IoT Data Generator
"""
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Optional


class MqttSettings(BaseModel):
    """MQTT broker configuration"""
    host: str = "mqtt_broker"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None


class PublisherSettings(BaseModel):
    """Publisher configuration"""
    topic: str = "sensors/readings"
    interval: int = 1
    qos: int = 1


class LoggingSettings(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class Settings(BaseSettings):
    """Application settings loaded from environment"""
    mqtt: MqttSettings = Field(default_factory=MqttSettings)
    publisher: PublisherSettings = Field(default_factory=PublisherSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    class Config:
        env_nested_delimiter = "__"
        case_sensitive = False
        env_file = ".env"


def get_settings() -> Settings:
    """Get settings instance"""
    return Settings()