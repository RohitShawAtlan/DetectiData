from connection import get_connection
from utils import search_tables,update_metadata
from pyatlan.model.assets import Asset, Table
import csv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

connection,client = get_connection("Rohit-Shaw")


# Open the CSV file
with open('day0.csv', mode='r', newline='') as file:
    reader = csv.reader(file)
    next(reader)
    # Iterate over each row in the CSV file
    for row in reader:
        database_name,schema_name,name = row[1].split('.')
        tables = search_tables(client,connection,database_name,schema_name,name)
        for table in tables:
            update_metadata(client,table,row)
