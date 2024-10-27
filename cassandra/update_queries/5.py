from cassandra.cluster import Cluster
from datetime import datetime

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Data for the new transaction
transaction_date = datetime(2024, 2, 1).date()
status = "paid"
customer_id = 10
transaction_id = 2000000  # Example transaction ID

# List of albums purchased in this transaction
transaction_albums = {
    1: "2|Album Title 1|150000",  # album_id: "quantity|title|price"
    2: "1|Album Title 2|120000"
}

# Measure start time
start_time = datetime.now()

# Insert transaction
session.execute("""
    INSERT INTO transactions (id, transaction_date, status, customer_id, transaction_albums)
    VALUES (%s, %s, %s, %s, %s);
""", (transaction_id, transaction_date, status, customer_id, transaction_albums))

# Measure end time
end_time = datetime.now()

# Calculate execution time in microseconds
execution_time_microseconds = (end_time - start_time).microseconds
print("Query M5 Transaksi pembelian beberapa album berhasil ditambahkan.")
print(f"Execution time (in microseconds): {execution_time_microseconds}")
