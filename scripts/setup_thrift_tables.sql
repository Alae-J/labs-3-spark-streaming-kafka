-- Register the HDFS Parquet layers as Spark SQL tables, so Superset / Beeline /
-- any HiveServer2 client can query them via JDBC on port 10000.
--
-- The tables are EXTERNAL: dropping them does not delete the underlying data.
-- USING parquet + LOCATION lets Spark auto-detect both schema and partitions.
--
-- Run this once after the Bronze/Silver/Gold layers have at least one Parquet
-- file written by notebook 04_hdfs_medallion.ipynb. From the host:
--
--   docker exec -i lab3-spark-thrift /opt/spark/bin/beeline \
--     -u jdbc:hive2://localhost:10000 -n spark \
--     -f /opt/scripts/setup_thrift_tables.sql

CREATE TABLE IF NOT EXISTS bronze_ticks
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/bronze/ticks';

CREATE TABLE IF NOT EXISTS silver_ticks
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/silver/ticks';

CREATE TABLE IF NOT EXISTS gold_moving_avg
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/gold/moving_avg';

-- Star-schema tables produced by notebook 07. Skipped silently
-- if the corresponding HDFS path does not yet exist.
CREATE TABLE IF NOT EXISTS fact_ticks
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/gold_star/fact_ticks';

CREATE TABLE IF NOT EXISTS dim_time
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/gold_star/dim_time';

CREATE TABLE IF NOT EXISTS dim_symbol
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/gold_star/dim_symbol';

CREATE TABLE IF NOT EXISTS dim_market
USING parquet
LOCATION 'hdfs://namenode:9000/lab3/gold_star/dim_market';

-- Partition discovery for tables created with partitionBy on write.
-- Without this, partitioned tables read 0 rows over Thrift.
MSCK REPAIR TABLE bronze_ticks;
MSCK REPAIR TABLE silver_ticks;
MSCK REPAIR TABLE gold_moving_avg;
MSCK REPAIR TABLE fact_ticks;

SHOW TABLES;
