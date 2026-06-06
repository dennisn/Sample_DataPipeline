"""VWAP calculation service"""

# Standard library imports
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
import pathlib

# Third-party imports
from kafka import KafkaProducer

# Local imports
import bootstrap
from data_defs import CryptoTradeData, CryptoVwap

# Constants
SERVICE_NAME = pathlib.Path(__file__).stem

WINDOW_SIZE = timedelta(minutes=1)  # 1-minute tumbling window


class TumblingWindowVWAPCalculator:
    """Helper class to calculate VWAP using tumbling windows."""
    def __init__(self, service_contaner: bootstrap.ServiceContainer):
        self.logger = service_container.root_logger
        self.consumer = service_container.kafka_consumer
        self.producer = service_container.kafka_producer
        # State Structure: { symbol: { window_start_time: { 'total_value': float, 'total_volume': float } } }
        self.windows = defaultdict[str, defaultdict[datetime, dict[str, float]]](lambda: defaultdict(lambda: {'total_value': 0.0, 'total_volume': 0.0}))
        # Keeps track of the latest event time per symbol to trigger window closing
        self.latest_event_time = defaultdict[str, datetime](lambda: datetime.min.replace(tzinfo=timezone.utc))
        
    def run_service(self):
        """Continuously polling the topic & calculate VWAP"""
        self.logger.info(f"Streaming calculation started. Listening to topic '{bootstrap.KAFKA_CRYPTO_TRADES_TOPIC_NAME}'...")
        self.consumer.subscribe(bootstrap.KAFKA_CRYPTO_TRADES_TOPIC_NAME)
        
        while True:
            msg_pack  = self.consumer.poll(timeout_ms=1000)
            if not msg_pack:
                continue
            
            try:
                for tp, messages in msg_pack.items():
                    if tp.topic != bootstrap.KAFKA_CRYPTO_TRADES_TOPIC_NAME:
                        continue
                    
                    for msg in messages:
                        msg_value = json.loads(msg.value.decode('utf-8'))
                        for payload in msg_value["data"]:
                            trade_data = CryptoTradeData.from_dict(payload)
                            self.process_trade(trade_data)
            except (ValueError, KeyError, TypeError) as parse_err:
                self.logger.warning(f"Skipping malformed message payload", exc_info=True)
                continue
        
        
    def get_window_start(self, dt: datetime) -> datetime:
        """Finds the start of the 1-minute tumbling window for a given timestamp."""
        discard = dt.second * 1000000 + dt.microsecond
        return dt - timedelta(microseconds=discard)
    
    
    def process_trade(self, trade_data: CryptoTradeData):
        """Processes an incoming trade record and evaluates window boundaries."""
        window_start = self.get_window_start(trade_data.event_time)
        
        # Advance the event time clock for this symbol
        if trade_data.event_time > self.latest_event_time[trade_data.symbol]:
            self.latest_event_time[trade_data.symbol] = trade_data.event_time
            
        # Emit older closed windows for this symbol if the clock moved past them
        self.flush_closed_windows(trade_data.symbol, window_start)

        # Update the active window aggregations
        self.windows[trade_data.symbol][window_start]['total_value'] += trade_data.price * trade_data.quantity
        self.windows[trade_data.symbol][window_start]['total_volume'] += trade_data.quantity
        
        
    def flush_closed_windows(self, symbol: str, current_window_start: datetime):
        """Emits and deletes windows older than the current active window."""
        closed_starts = [t for t in self.windows[symbol] if t < current_window_start]
        
        for start_time in sorted(closed_starts):
            metrics = self.windows[symbol].pop(start_time)
            if metrics['total_volume'] > 0:
                vwap = metrics['total_value'] / metrics['total_volume']
                end_time = start_time + WINDOW_SIZE
                
                output_payload = CryptoVwap(
                    symbol=symbol,
                    vwap=round(vwap, 4),
                    total_volume=round(metrics['total_volume'], 4),
                    window_start=start_time,
                    window_end=end_time
                )
                
                # Produce calculation result to Kafka
                self.logger.info(f"Emitting VWAP for {symbol} [{start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}]: {vwap:.2f} with volume {metrics['total_volume']:.4f}")
                # try:
                #     producer.produce(
                #         topic=OUTPUT_TOPIC,
                #         key=symbol,
                #         value=json.dumps(output_payload),
                #         callback=self.delivery_report
                #     )
                #     # Poll to trigger delivery callbacks
                #     producer.poll(0)
                #     logging.info(f"Emitted VWAP for {symbol} [{start_time.strftime('%M:%S')}]: {vwap:.2f}")
                # except Exception as e:
                #     logging.error(f"Failed to produce message: {e}")


if __name__ == "__main__":
    with bootstrap.ServiceContainer(service_name=SERVICE_NAME) as service_container:
        vwap_calculator = TumblingWindowVWAPCalculator(service_container)
        
        try:
            vwap_calculator.run_service()
        except KeyboardInterrupt:
            service_container.root_logger.info("Shutting down gracefully...")
