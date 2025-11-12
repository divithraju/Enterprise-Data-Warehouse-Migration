#!/usr/bin/env python3
"""Sales ETL PySpark job
Reads Parquet from HDFS raw path, performs cleaning, enrichment, aggregations, top-N, writes Parquet partitioned by region and year."""
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, year, when, row_number, sum as _sum, avg as _avg

cfg = yaml.safe_load(open('config/pipeline_config.yaml'))
HDFS_RAW = cfg['raw_path'] + '/sales_transactions'
HDFS_PROCESSED = cfg['processed_path'] + '/sales_transactions'

def get_spark():
    return SparkSession.builder.appName(cfg['spark']['app_name']).getOrCreate()

def main():
    spark = get_spark()
    # Read raw parquet (sqoop imports as parquet). If testing locally, can read CSV fallback.
    try:
        df = spark.read.parquet(HDFS_RAW)
    except Exception as e:
        # fallback to reading CSV for local demo
        df = spark.read.option('header','true').csv('data/input/sales_transactions.csv')

    # Type casting and cleaning
    df = df.withColumn('transaction_id', col('transaction_id').cast('int'))            .withColumn('customer_id', col('customer_id').cast('int'))            .withColumn('amount', col('amount').cast('double'))            .withColumn('last_updated', to_date(col('last_updated')))

    df = df.dropDuplicates(['transaction_id']).filter(col('transaction_id').isNotNull())

    # Enrichments
    df = df.withColumn('year', year(col('last_updated')))            .withColumn('spend_band', when(col('amount') < 1000, 'Low').when(col('amount') < 5000, 'Medium').otherwise('High'))            .withColumn('region', col('region'))

    # Aggregations
    agg = df.groupBy('region','year').agg(_sum('amount').alias('total_sales'), _avg('amount').alias('avg_sales'))

    # Top 5 customers by amount per region
    window = Window.partitionBy('region').orderBy(col('amount').desc())
    top_customers = df.withColumn('rn', row_number().over(window)).filter(col('rn') <= 5).drop('rn')

    # Write outputs
    df.write.mode('overwrite').partitionBy('region','year').parquet(HDFS_PROCESSED)
    agg.write.mode('overwrite').parquet(cfg['processed_path'] + '/sales_region_year_agg')
    top_customers.write.mode('overwrite').parquet(cfg['processed_path'] + '/top_customers_by_region')

    spark.stop()
    print('ETL complete.')

if __name__ == '__main__':
    main()
