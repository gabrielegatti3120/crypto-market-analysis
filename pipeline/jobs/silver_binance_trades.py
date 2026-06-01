from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, LongType, BooleanType, StructType, StructField
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

def run_trades_silver():

    app_name = "silver_trades"
    spark = create_spark_session(app_name)

    schema = ArrayType(
        StructType([
            StructField("id", LongType()),
            StructField("price", StringType()),
            StructField("qty", StringType()),
            StructField("quoteQty", StringType()),
            StructField("time", LongType()),
            StructField("isBuyerMaker", BooleanType()),
            StructField("isBestMatch", BooleanType())
        ])
    )

    df = spark.read.parquet("s3a://bronze/binance/trades/")

    df_parsed = df.withColumn(
        "arr",
        from_json(col("bronze_source_data"), schema)
    )

    df_exploded = df_parsed.withColumn(
        "trade",
        explode("arr")
    )

    df_silver = df_exploded.select(
        col("symbol"),
        (col("trade.time") / 1000).cast("timestamp").alias("trade_time"),
        col("trade.id").alias("trade_id"),
        col("trade.price").cast("double").alias("trade_price"),
        col("trade.qty").cast("double").alias("trade_qty"),
        col("trade.quoteQty").cast("double").alias("trade_quoteQty"),
        col("trade.isBuyerMaker").alias("isBuyerMaker"),
        col("trade.isBestMatch").alias("isBestMatch"),
        col("ingestion_ts")
    ).withColumn(
        "date",
        to_date("trade_time")
    )

    df_silver.write \
    .mode("append") \
    .partitionBy("date", "symbol") \
    .parquet("s3a://silver/binance/trades/")

    spark.stop()

if __name__ == "__main__":
    run_trades_silver()