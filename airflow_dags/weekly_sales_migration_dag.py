from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'divithraju',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG('weekly_sales_migration',
         default_args=default_args,
         description='Weekly ETL pipeline for sales transactions (MySQL -> HDFS -> Spark)',
         schedule_interval='0 2 * * 0',  # every Sunday at 02:00
         start_date=datetime(2024, 1, 1),
         catchup=False) as dag:

    t1_sqoop = BashOperator(
        task_id='sqoop_import',
        bash_command='bash /home/divithraju/Enterprise-Data-Warehouse-Migration/sqoop_ingestion/sqoop_import_sales.sh'
    )

    t2_spark = BashOperator(
        task_id='spark_etl',
        bash_command='spark-submit --master local[*] /home/divithraju/Enterprise-Data-Warehouse-Migration/spark_transform/sales_transform_job.py'
    )

    t3_validation = BashOperator(
        task_id='data_validation',
        bash_command='python3 /home/divithraju/Enterprise-Data-Warehouse-Migration/validation/data_validation.py'
    )

    t1_sqoop >> t2_spark >> t3_validation
