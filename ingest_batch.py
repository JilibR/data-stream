import concurrent.futures
import time
import requests
import datetime
from typing import List
from queue import Queue


SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
TIMEFRAMES = ["1d", "1h"]
TEMP_DIR = "data/tmp"


class IngestionBatchCrypto:
    """
        Class produce url and download historical data crypto
        in diffenrent timeframe to binance.vision

        https://data.binance.vision/?prefix=data/futures/um/daily/klines/BNBUSDT/1d/BNBUSDT-1h-2026-04-03.zip
    """

    def __init__(self, crypto: List[str], days_history: int = 10):
        self.crypto = [symbol.upper() for symbol in crypto]
        self.url = f"https://data.binance.vision/?prefix=data/futures/um/daily/klines"
        self.today = datetime.date.today()
        self.days_history = days_history
        self.download_queue = Queue()

    def _generate_urls(self) -> list:
        """Generate Urls history (3 symbols * 2 TF * 100 jours)"""
        urls = []
        dates = [(datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y-%m-%d") 
                 for i in range(1, self.days_history + 1)]

        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                for date_str in dates:
                    # build url history
                    url = f"{self.url}/{symbol}/{tf}/{symbol}-{tf}-{date_str}.zip"
                    urls.append(url)
        return urls
    

    def download_url(self, url):
        """Downloading zip file for pool worker and add to queue"""
        try:
            filename = url.split('/')[-1]
            local_path = TEMP_DIR / filename

            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(response.content)
                
                # Send local path file to the queue
                self.download_queue.put(local_path)
            else:
                print(f"Error {response.status_code} pour {filename}")
        except Exception as e:
            print(f"Download Issue: {url}: {e}")

