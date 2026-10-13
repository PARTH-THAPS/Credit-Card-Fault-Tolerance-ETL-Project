# Credit Card Fault Tolerance ETL Project 

A streaming ETL project on **Databricks** that ingests credit card transactions and fraud watchlist data, enriches them with customer data, and produces real-time fraud alerts, high-value transaction alerts, and windowed transaction metrics.

Built with **Lakeflow Declarative Pipelines** (PySpark), **Structured Streaming**, **Auto Loader**, **Kafka**, and **Unity Catalog**, following the **medallion architecture** (bronze, silver, gold).

---

## Architecture

```
Kafka topic: credit_card_transactions ──► bronze.transactions ──► silver.transactions ─┐
                                                                                       │
PostgreSQL (customers) ──► bronze.customers ──► silver.customers ──────────────────────┤
                                                                                       ├─► gold.fraud_card_alert ──► Fraud email notification
Auto Loader (JSON files in a Volume) ──► bronze.fraud_watchlist ──► silver.fraud_watchlist ┤
                                                                                       ├─► gold.high_value_transactions ──► High-value email notifier
                                                                                       └─► gold.transaction_count_by_minute (+ sliding window)
```

### Data sources

| Source | Method | Target |
|---|---|---|
| Credit card transactions | Kafka stream (`credit_card_transactions` topic) | `fineguard.bronze.transactions` |
| Fraud watchlist | Auto Loader (`cloudFiles`, JSON) from `/Volumes/fineguard/source/fraud_watchlist/source_data/` | `fineguard.bronze.fraud_watchlist` |
| Customers | Lakeflow ingestion pipeline from PostgreSQL (`fineguard_postgres` connection) | `fineguard.bronze.customers` |




<img width="845" height="292" alt="image" src="https://github.com/user-attachments/assets/974f7158-6a62-40e6-b341-ba8bfb34f65e" />




### Layers

- **Bronze:** raw ingested data (transactions, fraud watchlist, customers).
- **Silver:** cleaned and validated data (transactions, fraud watchlist, customers), with data quality expectations.
- **Gold:** business-ready outputs:
  - Fraud card alerts (transactions matched against the watchlist)
  - High-value transaction alerts
  - Transaction counts per minute
  - Transaction counts over a 5-minute sliding window (1-minute slide, 5-minute watermark)

### Alerts

- **Fraud email notification:** sent when a transaction matches a watchlist entry.
- **High-value transaction email notifier:** sent when a transaction exceeds the configured threshold.

---

## Project structure

```
Credit-Card-Fault-Tolerance-ETL-Project/
└── fineguard_project/
    ├── fineguard_customers_silver_ingestion_2/   # Customers silver pipeline
    ├── fineguard_streaming/                      # Main streaming pipeline
    │   ├── bronze/                               # fraud_watchlist_bronze.py, transaction_bronze.py
    │   ├── silver/                               # customers_silver.py, transactions and watchlist silver
    │   ├── gold/                                 # sliding-window counts, high-value transactions
    │   └── alert/                                # fraud and high-value email notifiers
    ├── fraud_watchlist_file_generator/           # Generates sample watchlist JSON files
    ├── 01_kafka_streaming_test                   # Notebook: test the Kafka connection
    ├── 02_Setup_Secret_Scope                     # Notebook: create the secret scope
    ├── 04_autoloader                             # Notebook: Auto Loader prototype
    └── send_email                                # Notebook: email helper
```

---

## Pipelines

| Pipeline | Type | Purpose |
|---|---|---|
| `fineguard_customers_ingestion` | Ingestion pipeline | PostgreSQL `customers` table to `fineguard.bronze` |
| `fineguard_customers_silver_ingestion_2` | ETL pipeline | Cleans customers into `fineguard.silver.customers` |
| `fineguard_streaming` | ETL pipeline | Transactions and watchlist through bronze, silver, gold, and alerts |

---

## Prerequisites

- A Databricks workspace (Free Edition works, with the limits below)
- Unity Catalog with catalog `fineguard` and schemas `source`, `bronze`, `silver`, `gold`
- A Kafka cluster with a `credit_card_transactions` topic
- A PostgreSQL database with a `customers` table, and a Databricks connection named `fineguard_postgres`
- A Databricks secret scope holding credentials (see `02_Setup_Secret_Scope`)
- An email service or SMTP credentials for the alert notebooks

---

