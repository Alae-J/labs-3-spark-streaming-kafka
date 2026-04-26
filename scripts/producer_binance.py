"""
Binance -> Kafka producer (Spark job).

Polls the public Binance REST API every POLL_INTERVAL_SECONDS for a basket of
crypto symbols, normalises the response into {symbol, price, volume, timestamp}
and publishes one JSON message per symbol to the `financial_data` Kafka topic.

Usage (from inside lab3-spark-client):

  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/scripts/producer_binance.py
"""
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"]
KAFKA_BROKERS = "kafka:9092"
TOPIC = "financial_data"
POLL_INTERVAL_SECONDS = 5
BINANCE_URL = "https://api.binance.com/api/v3/ticker/24hr"

SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("timestamp", LongType(), False),
])


def fetch_ticker(symbol: str) -> dict:
    r = requests.get(BINANCE_URL, params={"symbol": symbol}, timeout=5)
    r.raise_for_status()
    d = r.json()
    return {
        "symbol": d["symbol"],
        "price": float(d["lastPrice"]),
        "volume": float(d["volume"]),
        "timestamp": int(d["closeTime"]),
    }


def main():
    spark = (
        SparkSession.builder
        .appName("BinanceToKafkaProducer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Producer started -> topic={TOPIC}, symbols={SYMBOLS}, interval={POLL_INTERVAL_SECONDS}s")

    iteration = 0
    while True:
        iteration += 1
        records = []
        for sym in SYMBOLS:
            try:
                records.append(fetch_ticker(sym))
            except Exception as exc:
                print(f"[iter {iteration}] fetch failed for {sym}: {exc}")

        if not records:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        df = spark.createDataFrame(records, schema=SCHEMA)

        # Kafka expects two columns: `key` (bytes/string) and `value` (bytes/string).
        # Using the symbol as the key guarantees that all ticks for the same
        # symbol land on the same partition (preserving per-symbol order).
        out = df.select(
            col("symbol").cast("string").alias("key"),
            to_json(struct(*[col(f) for f in ("symbol", "price", "volume", "timestamp")])).alias("value"),
        )

        (
            out.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", TOPIC)
            .save()
        )

        sample = records[0]
        print(f"[iter {iteration}] published {len(records)} ticks "
              f"(sample: {sample['symbol']}={sample['price']} vol={sample['volume']})")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
