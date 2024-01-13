import csv
import json
import logging
from datetime import date

logging.basicConfig(level=logging.INFO)
        
def read_from_csv(file_path):
    with open(file_path, 'r+') as file:
        content = csv.reader(file, delimiter=",")
        header = next(content)
        logging.info("Receiving data...")
        contents = list(content)
    return contents

def serialize_date(obj):
    if isinstance(obj, date):
        return obj.isoformat()

def get_from_db(sql_file_path, conn):
    with open(sql_file_path, "r") as sql_file:
        sql_query = sql_file.read()
    logging.info("Executing the query...") 
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        query_data = cursor.fetchall()
    json_data = json.dumps(query_data, default=serialize_date)
    return json_data, query_data

