#!/usr/bin/env python3
import yaml, json, tempfile, subprocess, os
from datetime import datetime
from pyspark.sql import SparkSession

cfg = yaml.safe_load(open('config/pipeline_config.yaml'))
HDFS_PROCESSED = cfg['processed_path'] + '/sales_transactions'
HDFS_VALIDATION = cfg['validation_path']

spark = SparkSession.builder.appName('DataQuality').getOrCreate()
# Try reading parquet; if missing, report empty
try:
    df = spark.read.parquet(HDFS_PROCESSED)
    total = df.count()
    null_tx = df.filter(df.transaction_id.isNull()).count()
    total_amount = df.groupBy().sum('amount').collect()[0][0] or 0.0
except Exception as e:
    total = 0
    null_tx = 0
    total_amount = 0.0

report = {
    'timestamp': datetime.utcnow().isoformat(),
    'total_records_processed': int(total),
    'null_transaction_id': int(null_tx),
    'total_amount': float(total_amount)
}

# write report locally and attempt to put to HDFS
local_tmp = '/tmp/dq_report_' + datetime.utcnow().strftime('%Y%m%d%H%M%S') + '.json'
with open(local_tmp, 'w') as f:
    json.dump(report, f, indent=2)

# create HDFS validation path and put report
subprocess.run(['hdfs','dfs','-mkdir','-p', HDFS_VALIDATION], check=False)
subprocess.run(['hdfs','dfs','-put','-f', local_tmp, HDFS_VALIDATION + '/dq_report_' + datetime.utcnow().strftime('%Y%m%d%H%M%S') + '.json'], check=False)

print('Data validation report:', report)
spark.stop()
