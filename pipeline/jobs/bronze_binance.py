import os
import json
import time
from datetime import datetime

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS = os.getenv("MINIO_ACCESS")
MINIO_PASSWORD = os.getenv("MINIO_SECRET")
BRONZE_KLINES = "s3a://bronze/binance/klines"
BRONZE_TRADES = "s3a://bronze/binance/trades"

BINANCE_BASE_URL = "http://api.binance.com/api/v3"
INTERVAL = "1h"
LIMIT_KLINES = 500 # Historic maximum per call
LIMIT_TRADES = 1000 # Max number of trades per call

# Top 10 CoinGecko mappati su simboli Binance
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "DOGEUSDT", "TRXUSDT", "USDCUSDT",
    "ADAUSDT", "AVAXUSDT"
]

def create_spark_session() -> SparkSession:
    return(
        SparkSession.builder
        .appName("bronze_coingecko_markets")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def fetch_klines(symbol: str) -> list:
    url = f"{BINANCE_BASE_URL}/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": LIMIT_KLINES}
    print("Waiting for klines response")
    response = requests.get(url, params=params, timeout=30)
    print("klines downloaded")
    response.raise_for_status()
    time.sleep(0.1)  # rate limit
    return response.json()


def fetch_trades(symbol: str) -> list:
    url = f"{BINANCE_BASE_URL}/trades"
    params = {"symbol": symbol, "limit": LIMIT_TRADES}
    print("Waiting for treades response")
    response = requests.get(url, params=params, timeout=30)
    print("Treades downloaded")
    response.raise_for_status()
    time.sleep(0.1)  # rate limit
    return response.json()

def save_to_bronze(
        spark: SparkSession,
        data: dict,
        bucket: str,
        ingestion_ts: str,
        symbol: str,
        data_type: str
) -> None:
    raw_json = json.dumps(data)

    df = spark.createDataFrame(
        [(raw_json,)],
        schema=["bronze_source_data"]
    ) \
    .withColumn("ingeted_at", current_timestamp()) \
    .withColumn("ingestion_ts", lit(ingestion_ts)) \
    .withColumn("source", lit("binance")) \
    .withColumn("data_type", lit(data_type)) \
    .withColumn("symbol", lit(symbol))

    output_path = f"{bucket}/ingestion_ts={ingestion_ts}/symbol={symbol}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Written bronze layer for Binance {symbol} data")

def main() -> None:
    spark = create_spark_session()
    ingestion_ts = datetime.now().strftime("%Y%m%d_%H")

    try:
        for symbol in SYMBOLS:
            print(f"Fetching {symbol}...")
            klines = fetch_klines(symbol)
            trades = fetch_trades(symbol)

            save_to_bronze(spark, klines, BRONZE_KLINES, ingestion_ts, symbol, "klines")
            save_to_bronze(spark, trades, BRONZE_TRADES, ingestion_ts, symbol, "trades")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()