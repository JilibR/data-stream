# **Real-Time Crypto Data Lake (Medaillon Architecture)**

Implementing pipeline injestion streaming from Binance Websockets to DataLake local. 

## **Archititecture:**

### **Bronze:**

Binance WS ➡️ Python (Async) ➡️ Pydantic ➡️ Local Storage


## **Structure Data Lake:**

data/
└── bronze/                     # Données brutes (JSONL)
    └── dt=2024-05-20/          # Partition par date
        └── hr=19/              # Partition par heure
            ├── BTCUSDT.jsonl   # Un fichier par symbole
            └── ETHUSDT.jsonl



## **Table**

Ingest and move into table

Bronze: crypto.bronze.crypto

Transformation, Cleaning into table
    
Silver: crypto.silver.candle
        crypto.silver.metrics

## **Stack:**

python 3.11
pydantic -> validation type
databricks


## **Roadmap & Cloud Readiness**

#### **Project is prepared to deploy in Azure Databricks**

    [x] Ingestion Bronze (Local)

    [x] Transformation Silver (Nettoyage via PySpark)

    [ ] Calculs Gold (Corrélation inter-monnaies via DuckDB/SQL)

    [ ] Migration vers Azure Blob Storage (ADLS Gen2)

    [ ] Orchestration via Azure Data Factory
