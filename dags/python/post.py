import csv
import logging
from datetime import datetime
from python.tools import read_from_csv

logging.basicConfig(level=logging.INFO)
        
def read_file():
    try:
        file_path = '/opt/airflow/dags/csv/Trading.csv'
        return read_from_csv(file_path=file_path)
    except Exception as e:
        print(f"Error - {e}")

def transform_data():
    try:
        contents = read_file()
        logging.info("Tranforming data...") 
        data = [{"trade_id": int(row[0]),
                 "trade_date": datetime.strptime(row[1], '%Y-%m-%d').date(),
                 "trade_name": str(row[2]),
                 "trade_type": str(row[3]),
                 "trade_result": float(row[4])} for row in contents]
        return data
    except Exception as e:
        print(f"Error - {e}")