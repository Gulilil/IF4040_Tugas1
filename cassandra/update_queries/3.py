from cassandra.cluster import Cluster
from datetime import datetime, timedelta

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Set the date threshold for transactions older than 1 year
one_year_ago = datetime.now().date() - timedelta(days=365)

# Query to select transactions with status 'Cancelled' older than 1 year
transactions_to_delete = session.execute("""
    SELECT id FROM transactions
    WHERE status = 'Cancelled' AND transaction_date < %s;
""", (one_year_ago,))

# Delete each transaction based on the retrieved IDs
for transaction in transactions_to_delete:
    transaction_id = transaction.id
    session.execute("""
        DELETE FROM transactions WHERE id = %s;
    """, (transaction_id,))

print("Transaksi yang dibatalkan lebih dari 1 tahun lalu berhasil dihapus.")
