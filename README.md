# Q Company Data Pipeline

## Overview

Q Company operates both physical stores and an e-commerce platform. This project outlines a data pipeline designed to integrate data from various sources, including branch updates, sales transactions, and real-time app logs. The goal is to efficiently process this data, load it into a Hive data warehouse, and support marketing and B2B analysis.

## Project Goals

- Efficiently ingest and process raw sales data from multiple sources.
- Load data into a structured Hive data warehouse.
- Support analysis for marketing and B2B teams.
- Process real-time app logs using Spark streaming and store data on HDFS.

## Architecture

- **Local File System:** Stores raw CSV files before processing.
- **HDFS (Hadoop Distributed File System):** Scalable storage for raw and processed data.
- **Spark:** Data processing engine for transformation and loading.
- **Hive:** Data warehouse for structured querying.
- **Cron:** Schedules batch jobs.
- **Kafka:** Manages real-time data feeds.
![image](https://github.com/user-attachments/assets/6fc6fcc2-8561-4b18-9deb-7f155a6df8c7)

## Data Model

### Star Schema

- **Fact Table:** `sales_transactions_fact` (stores transactional data).
- **Dimension Tables:** 
  - `customer_dim`
  - `product_dim`
  - `sales_agent_dim`
  - `branch_dim`

## Scripts

### Data Ingestion and Processing

- **`ingestCode.py`**  
  Moves files from the local file system to HDFS, managing state to avoid re-processing.

- **`processing.py`**  
  Reads raw CSV files from HDFS, transforms the data, and writes it to Hive tables. Handles data cleaning, transformation, and incremental loading.

- **`daily_dump_hive.py`**  
  Generates a daily CSV report of sales by agent and product.

- **`producer.py`**  
  Sends streaming data to Kafka.

- **`streaming.ipynb`**  
  Processes streaming data from Kafka and stores it in HDFS partitioned by date and hour.

- **`compaction.py`**  
  Combines small files in HDFS into a single file to resolve the small files problem.

- **`hiveRepairPartition.py`**  
  Updates Hive to recognize new partitions.

## Automation

### Cron Jobs

- **`My_crontab.txt`**  
  Automates batch jobs:
  - Hourly ingestion (`ingestCode.py`)
  - Hourly processing (`processing.py`)
  - Daily reporting (`daily_dump_hive.py`)

- **`Crontab.txt`**  
  Automates streaming jobs:
  - Hourly compaction (`compaction.py`)
  - Hourly Hive partition repair (`hiveRepairPartition.py`)

## Future Enhancements

- **Schema Evolution:**
  - Implement explicit schema definitions or schema registries to handle schema changes in raw data.

- **Data Quality Checks:**
  - Introduce column-level, business rule, and custom validation checks.

- **Logging and Monitoring:**
  - Enhance logging with structured formats, different log levels, and performance metrics.

## Business Queries and Insights

- **Marketing Team:**
  - Most Selling Products
  - Most Redeemed Offers
  - Lowest Performing Cities (Online Sales)
  - Most Redeemed Offers per Product

- **B2B Team:**
  - Daily Sales Dump

- **Streaming Queries:**
  - Total Sales Amount by Payment Method
  - Most Popular Categories by Number of Events
  - Revenue by Customer
  - Hourly Sales Analysis

- For batch processing, use:
     ```bash
     python ingestCode.py
     python processing.py
     python daily_dump_hive.py
     ```
- For streaming, start Kafka and run:
     ```bash
     python producer.py
     jupyter notebook streaming.ipynb
     ```
