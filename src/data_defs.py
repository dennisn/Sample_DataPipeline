"""Crypto data definition"""

# Standard library imports
from dataclasses import dataclass
import datetime

# Third-party imports
import faust

@dataclass
class CryptoTradeData:
    symbol: str
    event_time: datetime.datetime
    price: float
    quantity: float

@dataclass
class CryptoVwap(faust.Record, serializer='json'):
    symbol: str
    vwap: float
    total_volume: float
    window_start: str
    window_end: str
    