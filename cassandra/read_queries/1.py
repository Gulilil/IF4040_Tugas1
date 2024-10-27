from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Change IP if necessary
session = cluster.connect("pedal")

print ("reading")

rows = session.execute("""
    SELECT transaction_date, status, transaction_albums FROM transactions WHERE customer_id = 1;
""")

for row in rows:
    print(row)