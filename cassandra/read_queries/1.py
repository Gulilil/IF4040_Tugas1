import datetime
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Change IP if necessary
session = cluster.connect("pedal")

print("Reading data...")

# Measure start time
start = datetime.datetime.now()

# Execute the query
rows = session.execute("""
    SELECT transaction_date, status, transaction_albums FROM transactions WHERE customer_id = 1 ALLOW FILTERING;
""")

# Measure end time
end = datetime.datetime.now()

duration = end-start

print("Query 1")
print(f"Execution time (in microseconds): {duration.microseconds}")

# # Print each row
# for row in rows:
#     print(row)