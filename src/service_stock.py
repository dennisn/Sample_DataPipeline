"""
Using Alpha Vantage to get stock data and write to Kafka topic.
This independent script is used to get data for SPY (SP500 index) as baseline data for the pipeline. 
It can be extended to get data for other stocks as well.
"""

# Standard library imports
from dataclasses import dataclass, asdict
import os
from pathlib import Path
import pprint
import requests
import time

# Third-party imports
from kafka import KafkaProducer

# Local imports
import bootstrap

# Constants
SERVICE_NAME = Path(__file__).stem
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

@dataclass
class StockDailyData:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


def get_daily_stock_data(symbol: str, timeout_seconds: int = 5) -> dict:
    """Fetch daily stock data for a given symbol from Alpha Vantage."""
    query_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={ALPHA_VANTAGE_API_KEY}'
    logger.info(f"Fetching daily stock data for {symbol} from Alpha Vantage...")
    response = requests.get(query_url, timeout=timeout_seconds)
    return response.json()

def get_daily_SPY_data(timeout_seconds: int = 5) -> dict:
    """Fetch daily stock data for `SPY` from Alpha Vantage."""
    return get_daily_stock_data(symbol='SPY', timeout_seconds=timeout_seconds)

def get_daily_stock_data_as_objects(symbol: str, timeout_seconds: int = 5) -> list[StockDailyData]:
    """Fetch daily stock data for a given symbol and return as list of StockDailyData objects."""
    data = get_daily_stock_data(symbol, timeout_seconds)
    ts_data = data.get('Time Series (Daily)', {})
    stock_data_list = []
    for date, daily_data in ts_data.items():
        stock_data = StockDailyData(
            date=date,
            open=float(daily_data['1. open']),
            high=float(daily_data['2. high']),
            low=float(daily_data['3. low']),
            close=float(daily_data['4. close']),
            volume=int(daily_data['5. volume'])
        )
        stock_data_list.append(stock_data)
    return stock_data_list

def send_daily_stock_data_to_kafka(producer: KafkaProducer, topic_name: str, stock_data_list: list[StockDailyData], batch_size: int = 15) -> int:
    """Send daily stock data to Kafka topic in batches."""
    total_messages_sent = 0
    batch_no = 0
    for i in range(0, len(stock_data_list), batch_size):
        batch = stock_data_list[i:i + batch_size]
        payload = {
            "timestamp": int(time.time()),
            "data": [asdict(stock_data) for stock_data in batch]
        }
        producer.send(topic_name, value=payload)
        total_messages_sent += len(batch)
        batch_no += 1
        logger.info(f"Sent batch {batch_no} with {len(batch)} messages to Kafka topic '{topic_name}'. Total messages sent so far: {total_messages_sent}")
    return total_messages_sent

def _print_daily_data(ts_data: dict) -> None:
    """Print the daily stock data."""
    keys = list(ts_data.keys())
    keys.sort()
    output_lines = []
    for date in keys:
        pprint.pprint(f"{date}= {ts_data[date]}", width=150)

if __name__ == "__main__":
    with bootstrap.ServiceContainer(service_name=SERVICE_NAME) as service_container:
        if (service_container.kafka_producer is None):
            raise RuntimeError("KafkaProducerService is not initialized.")
        
        logger = service_container.root_logger
        logger.info("--- Alpha Vantage Data ---")
        stock_data_list = get_daily_stock_data_as_objects(symbol='SPY')
        try:
            total_messages_sent = send_daily_stock_data_to_kafka(
                producer=service_container.kafka_producer.producer,
                topic_name=bootstrap.KAFKA_STOCK_TOPIC_NAME,
                stock_data_list=stock_data_list
            )
            logger.info(f"Total messages sent to Kafka: {total_messages_sent}")
        except Exception as e:
            logger.error(f"An error occurred while sending: {e}")
