"""Bootstrap script for initializing the basic services.
"""
import logging
import os

import dotenv
from logging.config import dictConfig

dotenv.load_dotenv()  # Load environment variables from .env file
LOG_DIR = os.getenv('LOG_DIR', 'logs')  # Default to 'logs' if not set


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

class DatabaseService:
    def execute(self):
        return "Data retrieved"

class ServiceContainer:
    """Dependency Injection Container."""
    def __init__(self, service_name: str):
        self.db = DatabaseService()
        self.root_logger = logging.getLogger(service_name)

def bootstrap_application(service_name: str) -> ServiceContainer:
    """Initializes logging and services."""
    dictConfig(LOGGING_CONFIG)
    service_container = ServiceContainer(service_name)
    service_container.root_logger.info(f"Bootstrapping application for service: {service_name}")
    return service_container
