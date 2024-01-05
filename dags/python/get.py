import json
import logging
from datetime import date
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)

def serialize_date(obj):
    if isinstance(obj, date):
        return obj.isoformat()

def retrieve_data():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        sql_file_path = "/opt/airflow/dags/sql/INGEST_DATA.sql"
        conn = postgres_hook.get_conn()

        with open(sql_file_path, "r") as sql_file:
            sql_query = sql_file.read()
        logging.info("Executing the query...") 
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            fetch = cursor.fetchall()

        data = json.dumps(fetch, default=serialize_date)
        return data
    except Exception as e:
        print(f"Error - {e}")