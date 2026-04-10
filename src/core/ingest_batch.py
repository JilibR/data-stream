import os
import zipfile
import requests
import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

class CryptoBatchIngestor:
    """

        Class download symbols historical data symbol and unzip file downloaded.
        Orchestrates multi-threaded historical data ingestion from Binance Vision.

        ThreadPool:
            - 6 workers for downloading -> put file .zip in <path>/tmp
            - 4 workers for unzipping -> put unzip file in <path>/batch
            
    """

    def __init__(self, symbols: list, days_history: int = 10):
        self.symbols = [s.upper() for s in symbols]
        self.base_url = "https://data.binance.vision/data/futures/um/daily/klines"
        self.days_history = days_history
        self.download_queue = Queue()
        
        # Paths management
        if self._is_databricks_env():
            self.root_dir = "/Volumes/crypto/bronze/binance_raw_data"
            self.temp_dir = f"{self.root_dir}/tmp"
            self.bronze_dir = f"{self.root_dir}/batch"
        else:
            self.root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.temp_dir = os.path.join(self.root_dir, "data", "tmp")
            self.bronze_dir = os.path.join(self.root_dir, "data", "bronze", "batch")

        os.makedirs(self.temp_dir, exist_ok=True)

    def _is_databricks_env(self):
        # Verify if we are in Databricks workspace
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    def _generate_urls(self):
        urls = []
        # Starting from yesterday as today's daily ZIP isn't available yet
        dates = [(datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y-%m-%d") 
                 for i in range(1, self.days_history + 1)]
        
        for symbol in self.symbols:
            for tf in ["1d", "1h"]:
                for d in dates:
                    urls.append(f"{self.base_url}/{symbol}/{tf}/{symbol}-{tf}-{d}.zip")
        return urls

    def download_worker(self, url):
        """Worker 1: Downloads and signals the queue"""
        try:
            filename = url.split('/')[-1]
            local_path = os.path.join(self.temp_dir, filename)
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(response.content)
                self.download_queue.put(local_path)
            else:
                # Silently skip if file doesn't exist on Binance server
                pass
        except Exception as e:
            print(f"[-] Download Error for {url}: {e}")

    def unzip_worker(self):
        """Worker 2: Unzips as files arrive in queue"""
        while True:
            file_path = self.download_queue.get()
            if file_path is None: 
                self.download_queue.task_done()
                break # Proper exit signal
            
            try:
                filename = os.path.basename(file_path)
                # Parse symbol and timeframe from filename
                parts = filename.replace(".zip", "").split('-')
                symbol, tf = parts[0], parts[1]
                
                target_dir = os.path.join(self.bronze_dir, symbol, tf)
                os.makedirs(target_dir, exist_ok=True)

                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(target_dir)
                
                os.remove(file_path)
                print(f"[+] Extracted and removed zip file : {filename}")
                
            except Exception as e:
                print(f"[!] Extraction Error for {file_path}: {e}")
            finally:
                self.download_queue.task_done()

    def run(self):
        urls = self._generate_urls()
        print(f"[*] Starting pipeline for {len(urls)} potential files...")

        # We use 4 threads for unzipping (CPU/IO) and 6 for downloading (Network)
        with ThreadPoolExecutor(max_workers=4) as unzip_pool:
            for _ in range(4):
                unzip_pool.submit(self.unzip_worker)

            with ThreadPoolExecutor(max_workers=6) as dl_pool:
                dl_pool.map(self.download_worker, urls)

            for _ in range(4):
                self.download_queue.put(None)

            # 4. Wait for the queue to be fully processed
            self.download_queue.join()
        
        print(f"[*] Batch ingestion completed.")

if __name__ == "__main__":
    # Test local
    ingestor = CryptoBatchIngestor(["BTCUSDT", "ETHUSDT"], days_history=5)
    ingestor.run()