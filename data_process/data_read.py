import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

DATA_CSV_DIR = os.path.join(os.getcwd(), "data_csv")

def execute_query(query: str):
  try:
      db = mysql.connector.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), database="prak_basdat_3")
      cursor = db.cursor()

      cursor.execute(query)
      result = cursor.fetchall()
      return result
  except Exception as e:
      print("Error to read data from MySQL table {table}: {e}")



if __name__ == "__main__":

  tables_to_get = ['songs', 'albums', 'groups', 'idols', 'countries', 'companies']
  
  for table in tables_to_get:
    read_query = f"SELECT * FROM {table}"
    desc_query = f"DESCRIBE {table}"

    # Get desc data
    columns = []
    desc_data = execute_query(desc_query)
    for column in desc_data:
       col_name = column[0]
       columns.append(col_name)

    data = execute_query(read_query)
    df = pd.DataFrame(data, columns=columns)

    # Store to file
    filename = os.path.join(DATA_CSV_DIR, f"{table}.csv")
    df.to_csv(filename, index=False)
    print(f"Successfully store {table} data in {filename}")
