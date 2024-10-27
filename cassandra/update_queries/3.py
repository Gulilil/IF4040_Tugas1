from cassandra.cluster import Cluster
from datetime import datetime, timedelta

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Set the date threshold for transactions older than 1 year
one_year_ago = datetime.now().date() - timedelta(days=365)

# Start timing
start_time = datetime.now()

# Query to select transactions with status 'Cancelled' older than 1 year (using ALLOW FILTERING)
transactions_to_delete = session.execute("""
    SELECT id FROM transactions
    WHERE status = 'Cancelled' AND transaction_date < %s ALLOW FILTERING;
""", (one_year_ago,))

# Delete each transaction based on the retrieved IDs
for transaction in transactions_to_delete:
    print("Deleting transaction with ID:", transaction.id)
    transaction_id = transaction.id
    session.execute("""
        DELETE FROM transactions WHERE id = %s;
    """, (transaction_id,))

# End timing
end_time = datetime.now()
duration = end_time - start_time

# Print execution time in microseconds
print("Query M3 Transaksi yang dibatalkan lebih dari 1 tahun lalu berhasil dihapus.")
print(f"Total execution time: ({duration.microseconds} microseconds)")
