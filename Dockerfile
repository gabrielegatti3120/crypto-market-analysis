FROM apache/airflow:3.2.1-python3.11
 
USER root
 
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
 
COPY pyproject.toml .
 
RUN uv pip install --system \
    apache-airflow-providers-docker \
    docker \
    mlflow \
    requests \
    pycoingecko \
    python-binance
 
USER airflow
WORKDIR /opt/airflow