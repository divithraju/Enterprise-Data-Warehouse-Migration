CREATE DATABASE IF NOT EXISTS retail_sales;
USE retail_sales;

DROP TABLE IF EXISTS sales_transactions;
CREATE TABLE sales_transactions (
  transaction_id INT PRIMARY KEY,
  customer_id INT,
  product_category VARCHAR(100),
  amount DECIMAL(10,2),
  region VARCHAR(50),
  payment_mode VARCHAR(50),
  last_updated DATETIME
);
-- Example load (local):
-- LOAD DATA LOCAL INFILE 'data/input/sales_transactions.csv' INTO TABLE retail_sales.sales_transactions
-- FIELDS TERMINATED BY ',' IGNORE 1 LINES (transaction_id,customer_id,product_category,amount,region,payment_mode,last_updated);
