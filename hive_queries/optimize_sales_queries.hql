-- Hive optimization hints and settings
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;
-- Consider bucketing if doing heavy joins:
-- CLUSTERED BY (customer_id) INTO 32 BUCKETS
