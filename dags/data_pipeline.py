import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from python.post import read_file, transform_data
from python.get import retrieve_data
from python.plot import analyzing_data, plot_model

VERSION = '1.0.0'

data = transform_data()

dag_name = "data_pipeline"

default_args = {
    "owner": "admin"
}

with DAG(dag_name, default_args=default_args, start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    get_data = PythonOperator(
        task_id="get_data",
        retries=1,
        python_callable=read_file,
        trigger_rule="all_success",
        dag=dag
    )

    with TaskGroup('processing_data') as processing_data:
        create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="postgres",
            retries=1,
            trigger_rule="all_success",
            sql="sql/CREATE_TABLE.sql",
            dag=dag
        )

        insert_data = PostgresOperator(
            task_id="insert_data",
            postgres_conn_id="postgres",
            retries=1,
            trigger_rule="all_success",
            sql="sql/INSERT_DATA.sql",
            parameters={'trade_id': [value['trade_id'] for value in data],
                        'trade_date': [value['trade_date'] for value in data],
                        'trade_name': [value['trade_name'] for value in data],
                        'trade_type': [value['trade_type'] for value in data],
                        'trade_result': [value['trade_result'] for value in data]},
            dag=dag
        )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        retries=1,
        python_callable=retrieve_data,
        trigger_rule="all_success",
        dag=dag
    )

    with TaskGroup('explore_data') as explore_data:
        analyse_data = PythonOperator(
            task_id="analyse_data",
            retries=1,
            python_callable=analyzing_data,
            trigger_rule="all_success",
            dag=dag
        )

        plot_data = PythonOperator(
            task_id="plot_data",
            retries=1,
            python_callable=plot_model,
            trigger_rule="all_success",
            dag=dag
        )

    generate_report = EmailOperator(
        task_id="generate_report",
        to="drrrrragoss@gmail.com",
        subject="Airflow Report Regarding The Analysis",
        files=['/opt/airflow/dags/csv/Report.csv',
            '/opt/airflow/dags/csv/Analysis.csv',
            '/opt/airflow/dags/images/PairPlot.png',
            '/opt/airflow/dags/images/HistoryProgress.png',
            '/opt/airflow/dags/images/HeatMap.png'],
        html_content="""
                    <h2>Attached is a report analysis of the transactions made</h2>
                    <p>There you will find an overview of the data in order <br>
                    to make new decisions and understand the market</p>
                    """,
        dag=dag
    )

logging.debug(f'DAG {dag_name} - VERSION: {VERSION}')

get_data >> [processing_data] >> ingest_data >> [explore_data] >> generate_report
