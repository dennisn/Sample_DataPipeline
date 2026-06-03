"""Using websockets to access & stream real-time cryptocurrency data from Binance and write to Kafka topic.
This independent script is used to get real-time data for BTCUSDT (Bitcoin/USDT pair) as an example.
"""

# Standard library imports
import asyncio
from asyncio.log import logger
from dataclasses import asdict, dataclass
import datetime
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
import websockets

# Third-party imports
from kafka import KafkaProducer

# Local imports
import bootstrap

# Constants
SERVICE_NAME = Path(__file__).stem

BINANCE_WS_BASE_URL = os.getenv('BINANCE_WS_BASE_URL', 'wss://stream.binance.com:9443/ws/')
BINANCE_WS_INITIAL_DELAY = float(os.getenv('BINANCE_WS_INITIAL_DELAY', '1.0'))
BINANCE_WS_MAX_DELAY = float(os.getenv('BINANCE_WS_MAX_DELAY', '60.0'))

KAFKA_CRYPTO_TRADES_TOPIC_NAME = os.getenv('KAFKA_CRYPTO_TRADES_TOPIC_NAME', 'crypto_trades')

@dataclass
class BinanceTradeData:
    symbol: str
    event_time: datetime.datetime
    price: float
    quantity: float

class BinanceTradeDataSender:
    """Helper class to send BinanceTradeData to Kafka topic in batches."""
    def __init__(self, service_container: bootstrap.ServiceContainer, topic_name: str):
        self.logger = service_container.root_logger
        self.kafka_producer = service_container.kafka_producer.producer
        self.topic_name = topic_name
        self.total_messages_sent = 0
        self.batch_no = 0

    async def send_biance_trade_data_to_kafka(self, trade_data_batch: list[BinanceTradeData]) -> None:
        """Send a batch of BinanceTradeData items to Kafka topic.

        Args:
            trade_data_batch (list[BinanceTradeData]): The batch of trade data to send.
        """
        try:
            payload = {
                "timestamp": int(time.time()),
                "data": [asdict(stock_data) for stock_data in trade_data_batch]
            }
            self.kafka_producer.send(self.topic_name, value=payload)
            self.batch_no += 1
            self.total_messages_sent += len(trade_data_batch)
            self.logger.info(f"Sent batch {self.batch_no} with {len(trade_data_batch)} messages to Kafka topic '{self.topic_name}'. Total messages sent so far: {self.total_messages_sent}")
        except Exception as exc:
            self.logger.error(f"Failed to send trade data to Kafka", exc_info=True)

    async def process_binance_trade_data(self, queue: asyncio.Queue, max_batch_size: int = 50, max_wait_seconds: float = 0.5) -> None:
        """Process a batch of BinanceTradeData and send them to Kafka

        Args:
            queue (asyncio.Queue): Async queue to store processed trade data.
            max_batch_size (int): Maximum number of items to include in each batch.
            max_wait_seconds (float): Maximum time to wait for new items before flushing the batch.
        """
        batch = []
        start_time = time.time()

        try:
            while True:
                try:
                    # Calculate remaining time window for the current batch
                    time_left = max_wait_seconds - (time.time() - start_time)
                    
                    # Wait for the next item from the stream
                    item = await asyncio.wait_for(queue.get(), timeout=max(0, time_left))
                    batch.append(item)
                    queue.task_done()
                except asyncio.TimeoutError:
                    # Triggered if max_wait_seconds passes without a new item
                    if batch:
                        self.logger.info(f"Timeout Flush! Batch ({len(batch)} items)")
                        await self.send_biance_trade_data_to_kafka(batch)  # Send the batch to Kafka
                        batch = []
                    start_time = time.time()
                    continue

                # Triggered if the batch reaches its maximum capacity
                if len(batch) >= max_batch_size:
                    self.logger.info(f"Size Flush! Batch ({len(batch)} items)")
                    await self.send_biance_trade_data_to_kafka(batch)  # Send the batch to Kafka
                    batch = []
                    start_time = time.time()
        except asyncio.CancelledError:
            self.logger.info(f"Consumption of binance trade data is shutting down after processing {self.total_messages_sent} messages.")
            raise

class BinanceTradeDataStreamer:
    """Helper class to stream Binance trade data from WebSocket and put it into an async queue."""
    def __init__(self, service_container: bootstrap.ServiceContainer):
        self.logger = service_container.root_logger
        self.total_messages_received = 0
        
    async def process_binance_message(self, queue: asyncio.Queue, message: websockets.Data) -> None:
        """Process a message received from Binance WebSocket, add it into the async queue for batch processing later."""
        try:
            data = json.loads(message)
            symbol = data.get('s')
            eventTime = data.get('E')
            price = data.get('p')
            quantity = data.get('q')
            local_dt = datetime.datetime.fromtimestamp(eventTime / 1000.0)
            utc_dt = datetime.datetime.fromtimestamp(eventTime / 1000.0, tz=datetime.UTC)
            if symbol and price and quantity and eventTime:
                self.logger.debug(f"{symbol} Last Price: ${price}, Quantity: {quantity}, Event Time: {local_dt} / {utc_dt}")
                self.total_messages_received += 1
                trade_data = BinanceTradeData(symbol=symbol, event_time=utc_dt, price=price, quantity=quantity)
                await queue.put(trade_data)
        except json.JSONDecodeError as exc:
            self.logger.error(f"Failed to decode message as JSON", exc_info=True)
        except Exception as exc:
            self.logger.error(f"Error processing message", exc_info=True)

    async def stream_binance_ticker(self, queue: asyncio.Queue, ticker_symbol: str = 'btcusdt'):
        """Stream real-time ticker data for a given symbol from Binance.

        Args:
            queue: Async queue to store processed trade data.
            ticker_symbol: Binance symbol to stream, e.g. 'btcusdt'.
        """
        if self.logger is None:
            raise RuntimeError("Missing logger.")

        # Reconnection/backoff parameters
        initial_delay = BINANCE_WS_INITIAL_DELAY
        max_delay = BINANCE_WS_MAX_DELAY
        backoff = initial_delay
        binance_ws_url = f"{BINANCE_WS_BASE_URL}{ticker_symbol}@trade"
        
        self.logger.info(f"Starting Binance stream for {ticker_symbol} at {binance_ws_url}")
        while True:
            try:
                self.logger.info(f"Connecting to Binance: {binance_ws_url}...")
                async with websockets.connect(binance_ws_url, ping_interval=20, ping_timeout=10) as websocket:
                    backoff = initial_delay  # reset backoff after successful connect
                    async for message in websocket:
                        await self.process_binance_message(queue, message)
            except asyncio.CancelledError:
                self.logger.info(f"Stopped receiving new data for ticker {ticker_symbol} after processing {self.total_messages_received} messages.")
                # Re-raise ensures the task cleanly exits its execution block
                raise 
            except Exception as exc:
                # Log and attempt reconnection with exponential backoff + jitter
                logger.error(f"WebSocket connection error", exc_info=True)
                sleep_time = min(backoff, max_delay)
                # add jitter
                jitter = random.uniform(0, sleep_time * 0.1)
                wait = sleep_time + jitter
                logger.info(f"Reconnecting in {wait:.1f} seconds...")
                try:
                    await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    raise
                backoff = min(backoff * 2, max_delay)

async def main(service_container: bootstrap.ServiceContainer):
    queue = asyncio.Queue()
    
    
    # start the Binance stream & processor in a background task
    binance_streamer = BinanceTradeDataStreamer(service_container)
    producer_tasks = [asyncio.create_task(binance_streamer.stream_binance_ticker(queue, 'btcusdt'))]
    
    binance_batch_sender = BinanceTradeDataSender(service_container, KAFKA_CRYPTO_TRADES_TOPIC_NAME)
    consumer_tasks = [asyncio.create_task(binance_batch_sender.process_binance_trade_data(queue))]
    
    try:
        # Keep the application running under normal operation
        # This acts as your application's main loop execution block
        await asyncio.gather(*producer_tasks)
        
    except KeyboardInterrupt:
        service_container.root_logger.info("\n[Shutdown] KeyboardInterrupt detected! Starting graceful drain...")
        
        # Step 1: Stop all producers immediately so no new work enters the queue
        for task in producer_tasks:
            task.cancel()
        
        # Ensure producer cancellations are fully processed by the event loop
        await asyncio.gather(*producer_tasks, return_exceptions=True)
        
        # Step 2: Join the queue to allow workers to finish remaining items
        service_container.root_logger.info(f"[Shutdown] Draining {queue.qsize()} items left in the queue...")
        await queue.join()
        service_container.root_logger.info("[Shutdown] All queue items have been successfully processed.")
        
    finally:
        # Step 3: Clean up background workers once the queue is empty
        service_container.root_logger.info("[Shutdown] Stopping active worker tasks...")
        for task in consumer_tasks:
            task.cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)
        service_container.root_logger.info("[Shutdown] System offline.")
        

if __name__ == "__main__":
    with bootstrap.ServiceContainer(service_name=SERVICE_NAME) as service_container:
        try:
            asyncio.run(main(service_container))
        except KeyboardInterrupt:
            # Prevents a messy traceback print to the console when exiting python
            service_container.root_logger.info("Program closed.")
            sys.exit(0)