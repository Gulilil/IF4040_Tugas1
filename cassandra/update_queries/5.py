from cassandra.cluster import Cluster
from datetime import datetime

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Data transaksi baru
transaction_date = datetime(2024, 2, 1).date()
status = "paid"
customer_id = 10
transaction_id = 2000000  # Contoh ID transaksi

# List album yang dibeli dalam transaksi ini
transaction_albums = [
    (1, 2, "Album Title 1", 150000),  # album_id, quantity, title, price
    (2, 1, "Album Title 2", 120000)
]

# Insert transaction
session.execute("""
    INSERT INTO transactions (id, transaction_date, status, customer_id, transaction_albums)
    VALUES (%s, %s, %s, %s, %s);
""", (transaction_id, transaction_date, status, customer_id, transaction_albums))

print("Transaksi pembelian beberapa album berhasil ditambahkan.")
