CREATE DATABASE IF NOT EXISTS sales_warehouse;
USE sales_warehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS sales_summary (
  transaction_id INT,
  customer_id INT,
  product_category STRING,
  amount DOUBLE,
  payment_mode STRING,
  last_updated DATE
)
PARTITIONED BY (region STRING, year INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:4444/user/divithraju/hr_project/processed/sales_transactions';
