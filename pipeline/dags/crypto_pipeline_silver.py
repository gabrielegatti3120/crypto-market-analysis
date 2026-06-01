import os
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import timedelta
import pendulum

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_PATH = "/opt/spark/jobs"       # path DENTRO il container Spark
HOST_JOBS_PATH = os.getenv("HOST_JOBS_PATH")  # path sull'host


def spark_operator(task_id: str, job: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image="custom-spark:latest",
        command=f"""
            /opt/spark/bin/spark-submit
            --master {SPARK_MASTER}
            --conf spark.submit.deployMode=client
            --conf spark.pyspark.python=python3
            --conf spark.pyspark.driver.python=python3
            --conf spark.executorEnv.PYSPARK_PYTHON=python3
            --name {task_id}
            {SPARK_JOBS_PATH}/{job}
        """,
        network_mode="crypto-net",              # stessa rete di spark-master
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=HOST_JOBS_PATH,
                target=SPARK_JOBS_PATH,
                type="bind",
            )
        ],
        environment={
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3",
            "MINIO_ENDPOINT": os.environ["MINIO_ENDPOINT"],
            "MINIO_ACCESS": os.environ["MINIO_ACCESS"],
            "MINIO_SECRET": os.environ["MINIO_SECRET"],
            "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI"],
        },
    )

@dag(
        dag_id="crypto_pipeline_silver",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["crypto", "silver", "medallion"],
        max_active_tasks=2,
        dagrun_timeout=timedelta(hours=2)
)
def crypto_pipeline_silver():

    silver_binance_klines = spark_operator(
        task_id="silver_binance_klines",
        job="silver_binance_klines.py",
    )

    silver_binance_trades = spark_operator(
        task_id="silver_binance_trades",
        job="silver_binance_trades.py",
    )

    silver_coingecko = spark_operator(
        task_id="silver_coingecko",
        job="silver_coingecko.py",
    )

    [silver_binance_klines, silver_binance_trades, silver_coingecko]

crypto_pipeline_silver()