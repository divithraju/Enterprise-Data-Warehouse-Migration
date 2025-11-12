#!/bin/bash
set -euo pipefail
# Production-style Sqoop incremental import (MySQL -> HDFS)
MYSQL_HOST="localhost"
MYSQL_PORT=3306
MYSQL_DB="retail_sales"
MYSQL_USER="divithraju"
MYSQL_PASS="divith4321"
TABLE="sales_transactions"
HDFS_TARGET_DIR="/user/divithraju/hr_project/raw/sales_transactions"
CHECK_COL="last_updated"
LAST_VALUE_FILE="/user/divithraju/hr_project/meta/sales_last_value.txt"
NUM_MAPPERS=4

hdfs dfs -mkdir -p ${HDFS_TARGET_DIR} || true
hdfs dfs -mkdir -p /user/divithraju/hr_project/meta || true

# If last value file does not exist on HDFS, use a safe default
if ! hdfs dfs -test -e ${LAST_VALUE_FILE}; then
  echo "2024-01-01 00:00:00" | hdfs dfs -put - ${LAST_VALUE_FILE} || true
fi

LAST_VALUE=$(hdfs dfs -cat ${LAST_VALUE_FILE} || echo "2024-01-01 00:00:00")

echo "Starting Sqoop incremental import. Last value: ${LAST_VALUE}"

sqoop import   --connect jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}   --username ${MYSQL_USER} --password ${MYSQL_PASS}   --table ${TABLE}   --target-dir ${HDFS_TARGET_DIR}   --as-parquetfile   --split-by transaction_id   --num-mappers ${NUM_MAPPERS}   --incremental append   --check-column ${CHECK_COL}   --last-value "${LAST_VALUE}"   --compress   --compression-codec org.apache.hadoop.io.compress.SnappyCodec   --null-string '\\N' --null-non-string '\\N'

# After import, compute the max(last_updated) from imported files and update last value
MAX_VAL=$(hdfs dfs -cat ${HDFS_TARGET_DIR}/* | awk -F',' '{print $7}' | sort | tail -n 1 || true)
if [ -n "${MAX_VAL}" ]; then
  echo "${MAX_VAL}" | hdfs dfs -put -f - ${LAST_VALUE_FILE}
  echo "Updated last value to ${MAX_VAL}"
fi

echo "Sqoop import finished."
