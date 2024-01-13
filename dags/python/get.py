import json
import logging
from datetime import date
from python.tools import get_from_db
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)

def retrieve_data():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        sql_file_path = "/opt/airflow/dags/sql/INGEST_DATA.sql"
        conn = postgres_hook.get_conn()

        json_data, _ = get_from_db(sql_file_path=sql_file_path, conn=conn)
        return json_data
    except Exception as e:
        print(f"Error - {e}")