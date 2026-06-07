"""Bootstrap script for initializing the basic services.
"""

# Standard library imports
from datetime import datetime, date
import json
import logging
import os
from time import time

import dotenv
from logging.config import dictConfig

# Third-party imports
from kafka import KafkaProducer, KafkaConsumer

dotenv.load_dotenv()  # Load environment variables from .env file
LOG_DIR = os.getenv('LOG_DIR', 'logs')  # Default to 'logs' if not set
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
KAFKA_STOCK_TOPIC_NAME = os.getenv('KAFKA_STOCK_TOPIC_NAME', 'stock_eod_ohlc_volume')
KAFKA_CRYPTO_TRADES_TOPIC_NAME = os.getenv('KAFKA_CRYPTO_TRADES_TOPIC_NAME', 'crypto_trades')
KAFKA_CRYPTO_VWAP_TOPIC_NAME = os.getenv('KAFKA_CRYPTO_VWAP_TOPIC_NAME', 'crypto_vwap')

# Define your centralized logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
        "ecs": {
            "()": "ecs_logging.StdlibFormatter",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "file_handler": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": f"{LOG_DIR}/app.log",
            "mode": "a",
            "encoding": "utf-8",
        },
        "ecs_file": {
            "class": "logging.FileHandler",
            "filename": f"{LOG_DIR}/app-ecs.json",
            "formatter": "ecs",
            "encoding": "utf-8",
        },
    },
    "root": {
        "handlers": ["console", "file_handler", "ecs_file"],
        "level": "INFO",
    },
}


class KafkaProducerService:
    def __init__(self):
        pass
        
    def __enter__(self):
        # Open resource when entering 'with' block
        self._producer = KafkaProducer(
            api_version=(2, 6, 0),
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=self.json_serial).encode(' utf-8')
            )
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up resource when exiting 'with' block
        self._producer.flush()
        self._producer.close()
    
    def json_serial(self, obj: object) -> str:
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    def send(self, topic: str, value) -> None:
        self._producer.send(topic, value)


class KafkaConsumerService:
    def __init__(self):
        pass

    def __enter__(self):
        # Open resource when entering 'with' block
        self._consumer = KafkaConsumer(
            api_version=(2, 6, 0),
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            )
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up resource when exiting 'with' block
        self._consumer.close()
        
    def subscribe(self, topic: str):
        self._consumer.subscribe(topic)
        
    def poll(self, timeout_ms: int):
        return self._consumer.poll(timeout_ms)
    

class ServiceContainer:
    """Dependency Injection Container."""
    def __init__(self, service_name: str):
        self.service_name = service_name
        dictConfig(LOGGING_CONFIG)
        self.root_logger = logging.getLogger(self.service_name)
        self.kafka_producer = KafkaProducerService()
        self.kafka_consumer = KafkaConsumerService()
        
    def __enter__(self) -> ServiceContainer:
        # Open resource when entering 'with' block
        self.kafka_consumer.__enter__()
        self.kafka_producer.__enter__()
        self.root_logger.info(f"Bootstrapping application for service: {self.service_name}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kafka_producer.__exit__(exc_type, exc_val, exc_tb)
        self.kafka_consumer.__exit__(exc_type, exc_val, exc_tb)
        self.root_logger.info(f"Shutting down application for service: {self.service_name}")

# if __name__ == "__main__":
#     with ServiceContainer(service_name="bootstrap_test") as service_container:
#         producer = service_container.kafka_producer.producer
#         message = {"user": "Alice", "status": "learning_redpanda", "timestamp": time()}
#         future = producer.send('foobar', message)
#         result = future.get(timeout=10)
#         producer.flush()
#         print("Message sent to Kafka topic 'foobar' with result: ", result)