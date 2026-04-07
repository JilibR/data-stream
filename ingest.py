import asyncio
import websockets
import logging
import json
import os
import datetime
from datetime import datetime
from pydantic import ValidationError
from models import BinanceMessage, SubscribeStream
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("API_KEY")
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
STREAMS = "/".join([f"{crypto}@aggTrade" for crypto in SYMBOLS])
URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")


async def ingest_ais_stream():
    if not API_KEY:
        logging.error(f"Error : API_KEY doesn't exist in .env")
        return

    logging.info(f"Init connection...")
    active_hour_window = None

    try:
        async with websockets.connect(URL, open_timeout=10.0) as websocket:
            logging.info(f"Connected to Binance websocket...")
            while True:
                raw_data = await websocket.recv()
                response = json.loads(raw_data)
                
                if 'result' in response:
                    try:
                        subscribe = SubscribeStream(**response)
                        logging.info(f"Stream confirmed ID: {subscribe.result}")
                        continue
                    except ValidationError:
                        pass
                
                try:
                    message = BinanceMessage(**response)
                    trade = message.data

                    timestamp = datetime.fromtimestamp(trade.timestamp/1000.0)
                    date_message = timestamp.strftime("%Y-%m-%d")
                    hour = timestamp.strftime("%H")

                    partition_path = os.path.join(BRONZE_DIR, f"dt{date_message}", f"hr={hour}")

                    if active_hour_window != hour:
                        active_hour_window = hour
                        os.makedirs(partition_path, exist_ok=True)

                    json_file_path = os.path.join(partition_path, f"{trade.symbol}.jsonl")
                    
                    try:
                        with open(json_file_path, "a") as file:
                            file.write(trade.model_dump_json() + "\n")
                    except PermissionError as e:
                        logging.error(f"Writing Error Permission : {e}")
                    except OSError as e:
                        logging.error(f"Writing Error : {e}")
                
                except ValidationError as e:
                    logging.warning(f"Message issue : {e}")

    except websockets.exceptions.ConnectionClosed as e:
        logging.error(f"Websocket connection error: {e}")
    except TimeoutError as e:
        logging.error(f"Failed to connect: {e}")
        


if __name__ == "__main__":
    asyncio.run(ingest_ais_stream())