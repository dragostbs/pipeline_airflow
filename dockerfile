FROM apache/airflow:latest

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt