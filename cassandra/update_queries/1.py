import datetime
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Start timing
start_time = datetime.datetime.now()

# Step 1: Select all albums that meet the criteria
albums_to_update = session.execute("""
    SELECT id, price FROM albums
    WHERE genre = 'Pop' AND release_date < '2020-01-01' ALLOW FILTERING;
""")

# Step 2: Update the price of each album
for album in albums_to_update:
    new_price = int(album.price * 1.10)  # Increase price by 10%
    session.execute(f"""
        UPDATE albums
        SET price = {new_price}
        WHERE id = {album.id};
    """)

# End timing
end_time = datetime.datetime.now()
duration = end_time - start_time

print("Query M1")
print(f"Harga album berhasil diperbarui. Total execution time: {duration.microseconds} microseconds.")
