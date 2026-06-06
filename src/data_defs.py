"""Crypto data definition"""

# Standard library imports
from dataclasses import dataclass
import datetime

# Third-party imports


def parse_timestamp(ts_str: str) -> datetime.datetime:
    """Parses standard ISO timestamps into a timezone-aware UTC datetime."""
    try:
        dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except ValueError:
        return datetime.datetime.now(datetime.timezone.utc)


@dataclass
class CryptoTradeData:
    symbol: str
    event_time: datetime.datetime
    price: float
    quantity: float
    
    @classmethod
    def from_dict(cls, data: dict):
        # Create a copy to avoid mutating the original input dictionary
        data_copy = data.copy()
        
        # Manually parse the string into a datetime object
        if isinstance(data_copy.get("event_time"), str):
            data_copy["event_time"] = parse_timestamp(data_copy["event_time"])
        
        if isinstance(data_copy.get("price"), str):
            data_copy["price"] = float(data_copy["price"])
            
        if isinstance(data_copy.get("quantity"), str):
            data_copy["quantity"] = float(data_copy["quantity"])
            
        return cls(**data_copy)


@dataclass
class CryptoVwap():
    symbol: str
    vwap: float
    total_volume: float
    window_start: datetime.datetime
    window_end: datetime.datetime
    