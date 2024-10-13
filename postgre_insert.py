import psycopg2
from dotenv import load_dotenv
import os
from postgre_queries_constant import *
import pandas as pd
import numpy as np

load_dotenv()

DATA_FINAL_DIR = os.path.join(os.getcwd(), "data_final")

def execute_query_return(query: str):
  # Establish the connection
  connection = None
  cursor = None
  try:
      connection = psycopg2.connect(
          database=os.getenv("POSTGRE_DB_NAME"),
          user=os.getenv("POSTGRE_DB_USER"),
          password=os.getenv("POSTGRE_DB_PASSWORD"),
          host=os.getenv("POSTGRE_DB_HOST"),
          port=os.getenv("POSTGRE_DB_PORT")
      )
      
      # Create a cursor object
      cursor = connection.cursor()
      
      # Execute a query
      cursor.execute(query)
      
      # Fetch all rows
      record = cursor.fetchall()
      
      print("[SUCCESS] Successfully run Query")
      return record
      
  except Exception as error:
      print("Error while running queries to PostgreSQL : ", error)
      
  finally:
      # Close the cursor and connection
      if cursor:
          cursor.close()  # Close the cursor
      if connection:
          connection.close()  # Close the connection

def execute_query(query: str):
  # Establish the connection
  connection = None
  cursor = None
  try:
      connection = psycopg2.connect(
          database=os.getenv("POSTGRE_DB_NAME"),
          user=os.getenv("POSTGRE_DB_USER"),
          password=os.getenv("POSTGRE_DB_PASSWORD"),
          host=os.getenv("POSTGRE_DB_HOST"),
          port=os.getenv("POSTGRE_DB_PORT")
      )
      
      # Create a cursor object
      cursor = connection.cursor()
      
      # Execute a query
      cursor.execute(query)
      
      # Fetch all rows
      connection.commit()
      print("[SUCCESS] Successfully run Query")
      
  except Exception as error:
      print("Error while running queries to PostgreSQL : ", error)
      
  finally:
      # Close the cursor and connection
      if cursor:
          cursor.close()  # Close the cursor
      if connection:
          connection.close()  # Close the connection

def adjusting_inputting_variable(val):
   if val is None or pd.isna(val):
      return "NULL"
   elif (isinstance(val, int)):
      return str(val)
   elif(isinstance(val, float)):
      return str(val)
   else:
      out_val = str(val).replace('\'', '\'\'')
      return f"\'{out_val}\'"

def construct_insert_queries(table: str, df: pd.DataFrame):
    cols = df.columns.tolist()

    col_string = ""
    for i in range(len(cols)):
      if (i == len(cols)-1):
          col_string += cols[i]
      else:
          col_string += f"{cols[i]}, "
    finalized_col_string = f"({col_string})"
        
    amount = 50000
    i = 0
    while (i*amount < len(df)):
      values_string = ""
      upperbound = min(len(df), (i+1) * amount)
      df_insert = df[i*amount: upperbound]
      try:
        for _, row in df_insert.iterrows():
          line_values_string = ""
          for j in range(len(cols)):
              attr = cols[j]
              if(j == len(cols)-1):
                  line_values_string += adjusting_inputting_variable(row[attr])
              else:
                  line_values_string += f"{adjusting_inputting_variable(row[attr])}, "
          values_string += f"({line_values_string}),\n"

        values_string = values_string[:-2] + ";"
        query = f"INSERT INTO {table}{finalized_col_string} VALUES {values_string}"
        
        # print(query)
        execute_query(query)
        print(f"[SUCCESS] Successfully insert {table} from {i*amount} to {upperbound}")
      except Exception as e:
        print(f"[FAILED] Failed to insert {table} from {i*amount} to {upperbound}: {e}")

      i += 1 #update

           
        

    
    return

if __name__ == "__main__":
  
  # # Create TYPE ENUM
  # TYPE_QUERIES = [GROUPS_TYPE_ENUM, IDOLS_GENDER_ENUM, TRANSACTIONS_STATUS_ENUM]
  # for query in TYPE_QUERIES:
  #   result = execute_query(query)
  #   print(result)

  # # Create TABLES
  # CREATE_TABLE_QUERIES = [COUNTRIES_CREATE, COMPANIES_CREATE, GROUPS_CREATE, IDOLS_CREATE,  ALBUMS_CREATE, SONGS_CREATE, CUSTOMERS_CREATE, TRANSACTIONS_CREATE, TRANSACTION_ALBUMS_CREATE]
  # for query in CREATE_TABLE_QUERIES:
  #   result = execute_query(query)
  #   print(result)

  data_list = ['countries', 'companies', 'groups', 'idols', 'albums', 'songs', 'customers', 'transactions', 'transaction_albums']
  for data_name in data_list[-1:]:
      data_path = os.path.join(DATA_FINAL_DIR, f"{data_name}.csv")
      df = pd.read_csv(data_path)
      construct_insert_queries(data_name, df)