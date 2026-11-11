
FROM apache/airflow:3.3.1

# Instalação do Java
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalação das dependências Python
USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.13.txt" \
    "apache-airflow==3.3.1" \
    apache-airflow-providers-fab \
    apache-airflow-providers-postgres \
    "pyspark==4.0.1" \
    "delta-spark==4.0.1"
