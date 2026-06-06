"""Crypto data definition"""

# Standard library imports
from dataclasses import dataclass
import datetime

# Third-party imports

@dataclass
class CryptoTradeData:
    symbol: str
    event_time: datetime.datetime
    price: float
    quantity: float

@dataclass
class CryptoVwap():
    symbol: str
    vwap: float
    total_volume: float
    window_start: datetime.datetime
    window_end: datetime.datetime
    