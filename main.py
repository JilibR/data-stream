import logging
from src.core.ingest_batch import CryptoBatchIngestor
from src.core.ingest_stream import CryptoStreamIngestor

# Config
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def main():
    logging.basicConfig(level=logging.INFO)
    
    # 1. Run Batch
    print("--- Starting Batch Ingestion ---")
    batch = CryptoBatchIngestor(SYMBOLS, days_history=3)
    batch.run()

    # 2. Run Stream
    print("--- Starting Real-Time Stream ---")
    stream = CryptoStreamIngestor(SYMBOLS)
    stream.run()

if __name__ == "__main__":
    main()