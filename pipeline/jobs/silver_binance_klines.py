from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
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

def run_klines_silver():

    app_name = "silver_klines"
    spark = create_spark_session(app_name)

    schema = ArrayType(ArrayType(StringType()))

    df = spark.read.parquet("s3a://bronze/binance/klines/")

    df_parsed = df.withColumn(
        "arr",
        from_json(col("bronze_source_data"), schema)
    )

    df_exploded = df_parsed.withColumn(
        "kline",
        explode("arr")
    )

    df_silver = df_exploded.select(
        col("symbol"),
        (col("kline")[0] / 1000).cast("timestamp").alias("open_time"),
        col("kline")[1].cast("double").alias("open"),
        col("kline")[2].cast("double").alias("high"),
        col("kline")[3].cast("double").alias("low"),
        col("kline")[4].cast("double").alias("close"),
        col("kline")[5].cast("double").alias("volume"),
        col("kline")[8].cast("int").alias("trades"),
        col("ingestion_ts")
    ).withColumn(
        "date",
        to_date("open_time")
    )

    df_silver.write \
    .mode("append") \
    .partitionBy("date", "symbol") \
    .parquet("s3a://silver/binance/klines/")

    spark.stop()

if __name__ == "__main__":
    run_klines_silver()