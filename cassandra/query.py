from cassandra.cluster import Cluster
import csv

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Change IP if necessary
session = cluster.connect("pedal")

print ("reading")

rows = session.execute("SELECT * FROM songs limit 1")

for row in rows:
    print(row)