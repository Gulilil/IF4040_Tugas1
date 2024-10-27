import psycopg2
from dotenv import load_dotenv
import os
from postgre_queries_constant import *
import pandas as pd
import numpy as np
import time

load_dotenv()

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

QUERY_1 = """SELECT t.transaction_date, a.title, ta.quantity
FROM transactions t
JOIN transaction_albums ta ON t.id = ta.transaction_id
JOIN albums a ON ta.album_id = a.id
JOIN customers c ON t.customer_id = c.id
WHERE c.name = 'Nostalgic Hermann';
"""

QUERY_2 = """SELECT g.name, SUM(ta.quantity) AS total_pembelian
FROM groups g
JOIN albums a ON g.id = a.group_id
JOIN transaction_albums ta ON a.id = ta.album_id
GROUP BY g.name
ORDER BY total_pembelian DESC;
"""

QUERY_3 = """SELECT a.title, a.price 
FROM albums a
JOIN groups g ON a.group_id = g.id
WHERE g.name = 'Stray Kids'
AND a.price BETWEEN 0 AND 500000
ORDER BY a.price ASC;
"""

QUERY_4 = """SELECT i.stage_name, i.full_name, i.date_of_birth, i.birthplace 
FROM idols i
JOIN groups g ON i.group_id = g.id
WHERE g.name = 'BABYMONSTER';
"""
QUERY_5 = """SELECT genre, MAX(total_pembelian) AS total_terjual FROM ( SELECT a.id, a.genre, a.title, SUM(ta.quantity) AS total_pembelian FROM albums AS a JOIN transaction_albums AS ta ON a.id = ta.album_id GROUP BY a.id, a.genre, a.title ) AS subquery GROUP BY genre;
"""

QUERY_6 = """UPDATE albums
SET price = price * 1.10
WHERE genre IN (
    SELECT DISTINCT genre 
    FROM albums 
    WHERE release_date < '2020-01-01'
      AND genre = 'Pop'
);
"""

QUERY_7 = """
WITH albums_to_update AS (
    SELECT id
    FROM albums
    WHERE genre = 'Pop'
    LIMIT 100
)
UPDATE albums
SET duration = (
    SELECT SUM(s.duration)
    FROM songs s
    WHERE s.album_id = albums.id
)
WHERE id IN (SELECT id FROM albums_to_update);
"""

QUERY_8 = """DELETE FROM transaction_albums
WHERE transaction_id IN (
    SELECT id FROM transactions 
    WHERE status = 'cancelled' 
      AND transaction_date < NOW() - INTERVAL '1 year'
);


DELETE FROM transactions
WHERE status = 'cancelled' AND transaction_date < DATE_SUB(NOW(), INTERVAL '1 YEAR');
"""

QUERY_9 = """DELETE FROM idols
WHERE id NOT IN (
    SELECT DISTINCT i.id
    FROM idols i
    WHERE i.group_id = null
);
"""

QUERY_10 = """INSERT INTO transactions (transaction_date, status, customer_id)
VALUES ('2024-02-01', 'paid', 10);

EXPLAIN ANALYZE
INSERT INTO transaction_albums (album_id, transaction_id, quantity)
VALUES 
(1, 2000000, 2),
(2, 2000000, 1);
"""

if __name__ == "__main__":
  QUERY_LIST = [QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10]

  for i in range(len(QUERY_LIST)):
    print(f"QUERY {i+1}")
    # Start time
    start = time.time() 

    if (i < 5):
      result = execute_query_return(QUERY_LIST[i])
    # else:
    #   execute_query(QUERY_LIST[i])    


    # End time
    end = time.time()
    duration = int(end-start)

    print(result)

    print(f"Execution time: {time.strftime('%H:%M:%S', time.gmtime(duration))}")
    