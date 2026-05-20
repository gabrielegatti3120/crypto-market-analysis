import os
import json
from datetime import datetime

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS = os.getenv("MINIO_ACCESS")
MINIO_PASSWORD = os.getenv("MINIO_SECRET")
BRONZE_BUCKET = "s3a://bronze/coingecko/markets"

COINGECKO_URL = "http://api.coingecko.com/api/v3/coins/markets"
COINGECKO_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": False
}

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

def fetch_coingecko() -> list:

    print(f"Fetching top {COINGECKO_PARAMS['per_page']} from page {COINGECKO_PARAMS['page']}...")

    response = requests.get(COINGECKO_URL, params=COINGECKO_PARAMS, timeout=30)
    response.raise_for_status()

    return response.json()

def save_to_bronze(
        spark: SparkSession,
        data: list,
        ingestion_ts: str
) -> None:
    raw_json = json.dumps(data)

    df = spark.createDataFrame(
        [(raw_json,)],
        schema=["bronze_source_data"]
    ) \
    .withColumn("ingeted_at", current_timestamp()) \
    .withColumn("ingestion_ts", lit(ingestion_ts)) \
    .withColumn("source", lit("coingecko")) 

    output_path = f"{BRONZE_BUCKET}/ingestion_ts={ingestion_ts}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Written bronze layer for Coingecko data")

def main() -> None:
    spark = create_spark_session()
    ingestion_ts = datetime.now().strftime("%Y%m%d_%H")

    try:
        data = fetch_coingecko()
        save_to_bronze(spark, data, ingestion_ts)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()