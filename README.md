## Enterprise Data Warehouse Migration from Legacy Systems

🚀 Overview

This project demonstrates a real-world enterprise data migration from a legacy RDBMS-based warehouse (MySQL) to a modern Hadoop-based data lake, enabling scalable, cost-efficient, and faster data analytics. 


## Tech Stack

| Layer         | Technology                 | Purpose                                        |
| ------------- | -------------------------- | ---------------------------------------------- |
| Storage       | **HDFS (Hadoop)**          | Distributed storage for raw and processed data |
| Ingestion     | **Sqoop**                  | Transfers data from MySQL → HDFS               |
| Processing    | **Apache Spark (PySpark)** | ETL transformation, cleaning, enrichment       |
| Warehouse     | **Apache Hive**            | Query and analytical access layer              |
| Orchestration | **Apache Airflow**         | Schedules and monitors weekly jobs             |
| Source        | **MySQL**                  | Simulated legacy database system               |
| Format        | **Parquet (Columnar)**     | Efficient storage for analytics                |

## ETL Flow

| Step             | Tool            | Description                                     |
| ---------------- | --------------- | ----------------------------------------------- |
| **1. Extract**   | Sqoop           | Imports data from MySQL → HDFS raw zone         |
| **2. Transform** | Spark (PySpark) | Cleans, aggregates, and normalizes data         |
| **3. Load**      | Hive            | Creates external Hive tables over Parquet files |
| **4. Validate**  | Python          | Data quality checks (nulls, duplicates, schema) |
| **5. Schedule**  | Airflow         | Weekly workflow orchestration and logging       |



