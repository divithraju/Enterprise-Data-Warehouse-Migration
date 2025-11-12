# Enterprise Data Warehouse Migration (Sales Transactions)

This project simulates migrating a legacy MySQL sales transactions table to a Hadoop data lake, transforming it with PySpark, and orchestrating weekly runs with Airflow.

Paths and credentials are configured in `config/pipeline_config.yaml` (do NOT commit secrets in public repos).

## Quickstart (for demo purposes)
1. Place `data/input/sales_transactions.csv` where convenient.
2. Create MySQL table using `mysql_scripts/create_sales_table.sql`
3. Upload initial CSV to MySQL or use LOAD DATA LOCAL INFILE.
4. Ensure Hadoop is running and HDFS paths exist (`hdfs dfs -mkdir -p /user/divithraju/hr_project/raw` etc.)
5. Run Sqoop import: `bash sqoop_ingestion/sqoop_import_sales.sh`
6. Run Spark ETL: `spark-submit spark_transform/sales_transform_job.py`
7. Validate: `python3 validation/data_validation.py`

The Airflow DAG `airflow_dags/weekly_sales_migration_dag.py` is configured to run weekly (Sunday 2 AM).
