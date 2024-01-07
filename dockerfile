FROM apache/airflow:latest

USER root

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt