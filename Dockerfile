FROM apache/airflow:2.11.2-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
RUN mkdir -p /opt/airflow/state
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.11.2" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt" \
    -r /requirements.txt
RUN mkdir -p /opt/airflow/ivy
COPY spark-defaults.conf /opt/airflow/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/airflow/spark-conf
# Avoid the base image's root-only CLI directory in runtime shell lookups.
ENV PATH="${SPARK_HOME}/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
RUN python -m pip check
