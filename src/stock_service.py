"""
Using Alpha Vantage to get stock data and write to Kafka topic.
This independent script is used to get data for SPY (SP500 index) as baseline data for the pipeline. It can be extended to get data for other stocks as well.
"""

import logging
import os
import pprint
import requests

import bootstrap

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

def get_daily_stock_data(symbol: str, timeout_seconds: int = 5) -> dict:
    """Fetch daily stock data for a given symbol from Alpha Vantage."""
    query_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={ALPHA_VANTAGE_API_KEY}'
    response = requests.get(query_url, timeout=timeout_seconds)
    return response.json()

def get_daily_SPY_data(timeout_seconds: int = 5) -> dict:
    """Fetch daily stock data for `SPY` from Alpha Vantage."""
    return get_daily_stock_data(symbol='SPY', timeout_seconds=timeout_seconds)

def print_daily_data(ts_data: dict) -> list[str]:
    """Print the daily stock data."""
    keys = list(ts_data.keys())
    keys.sort()
    output_lines = []
    for date in keys:
        line = f"{date}= {ts_data[date]}"
        output_lines.append(line)
        pprint.pformat(line, width=150)
    return output_lines

service_container = bootstrap.bootstrap_application(service_name="stock_service")
logger = service_container.root_logger
logger.info("--- Alpha Vantage Data ---")
# Alpha Vantage API endpoint for daily stock data
# data = get_daily_SPY_data()
# pprint.pprint("Meta data: ", data['Meta Data'])
# print("Time series data: ")
# print_daily_data(data['Time Series (Daily)'])
