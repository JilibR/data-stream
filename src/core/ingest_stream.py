import asyncio
import websockets
import logging
import os
from typing import List
from datetime import datetime
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed
from src.utils.models import BinanceMessage, SubscribeStream

# Config logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

STREAM_PATH = f"data/bronze/stream"

class CryptoStreamIngestor:
    """

        Orchestrates multi-threaded historical data ingestion from Binance Vision
        2 Pool Threads, one to download file, second to dezip files that managed
        Queue support by multithreading.

    """
    def __init__(self, symbols: List[str]):
        self.symbols = [s.lower() for s in symbols]
        self.streams = "/".join([f"{s}@aggTrade" for s in self.symbols])
        self.url = f"wss://stream.binance.com:9443/stream?streams={self.streams}"
        
        # Gestion du chemin racine (root)
        self.root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.bronze_dir = os.path.join(self.root_dir, STREAM_PATH)
        self.active_hour = None

    async def _save_trade(self, trade):
        """Partitionnement temporel et écriture JSONL"""
        try:
            ts = datetime.fromtimestamp(trade.timestamp / 1000.0)
            date_str, hour_str = ts.strftime("%Y-%m-%d"), ts.strftime("%H")

            path = os.path.join(self.bronze_dir, f"dt={date_str}", f"hr={hour_str}")
            
            if self.active_hour != hour_str:
                os.makedirs(path, exist_ok=True)
                self.active_hour = hour_str

            file_path = os.path.join(path, f"{trade.symbol.upper()}.jsonl")
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(trade.model_dump_json() + "\n")
        except Exception as e:
            logger.error(f"Write error: {e}")

    async def start(self):
        """Loop principa, reconnecting if lost connection"""
        while True:
            try:
                logger.info(f"Connecting to Binance...")
                async with websockets.connect(self.url) as ws:
                    logger.info("Stream active.")
                    while True:
                        raw = await ws.recv()
                        
                        try:
                            msg = BinanceMessage.model_validate_json(raw)
                            await self._save_trade(msg.data)
                        except ValidationError:
                            try:
                                sub = SubscribeStream.model_validate_json(raw)
                                logger.info(f"System: ID {sub.id} confirmed")
                            except ValidationError:
                                logger.warning("Unknown message format received")

            except (ConnectionClosed, Exception) as e:
                logger.error(f"Connection lost: {e}. Retry in 5s...")
                await asyncio.sleep(5)

    def run(self):
        """Launch via main.py"""
        try:
            asyncio.run(self.start())
        except KeyboardInterrupt:
            logger.info("Stopped by user.")

if __name__ == "__main__":
    CryptoStreamIngestor(["btcusdt", "ethusdt"]).run()