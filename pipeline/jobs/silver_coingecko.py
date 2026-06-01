from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, LongType, DoubleType, StructType, StructField
from pyspark.sql.functions import from_json, col, explode, to_date

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def run_coingecko_silver():

    app_name = "silver_coingecko"
    spark = create_spark_session(app_name)

    schema = ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("symbol", StringType()),
            StructField("current_price", DoubleType()),
            StructField("market_cap", DoubleType()),
            StructField("total_volume", DoubleType()),
            StructField("market_cap_rank", LongType())
        ])
    )

    df = spark.read.parquet("s3a://bronze/coingecko/markets/")

    df_parsed = df.withColumn(
        "arr",
        from_json(col("bronze_source_data"), schema)
    )

    df_exploded = df_parsed.withColumn(
        "asset",
        explode("arr")
    )

    df_silver = df_exploded.select(
        col("asset.id").alias("asset_id"),
        col("asset.symbol").alias("asset_symbol"),
        col("asset.current_price").alias("asset_current_price"),
        col("asset.market_cap").alias("asset_market_cap"),
        col("asset.total_volume").alias("asset_total_volume"),
        col("asset.market_cap_rank").alias("asset_market_cap_rank"),
        col("ingestion_ts")
    ).withColumn(
        "date",
        to_date(col("ingestion_ts"), "yyyyMMdd_HH")
    )

    df_silver.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet("s3a://silver/coingecko/markets/")

    spark.stop()


if __name__ == "__main__":
    run_coingecko_silver()