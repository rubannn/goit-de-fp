FROM apache/airflow:2.11.2-python3.12

RUN mkdir -p /opt/airflow/state
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.11.2" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt" \
    -r /requirements.txt
