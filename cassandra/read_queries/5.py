import datetime
from cassandra.cluster import Cluster
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Start timing from the beginning of the first query
start_time = datetime.datetime.now()
# Step 1: Retrieve all album transactions
album_rows = session.execute("""
    SELECT id, title, transaction_albums FROM albums;
""")
print("Query to retrieve all album transactions executed.")

# Step 2: Calculate total sales for each album
album_sales = defaultdict(int)

for album in album_rows:
    if album.transaction_albums:  # Check if transaction_albums is not None
        for transaction_id, quantity in album.transaction_albums.items():
            album_sales[album.id] += quantity  # Summing quantities by album.id

print("Sales calculation completed.")

# Step 3: Find the best-selling album
best_selling_album_id = max(album_sales, key=album_sales.get)
best_selling_album = session.execute(f"""
    SELECT id, title FROM albums WHERE id = {best_selling_album_id};
""").one()

# End timing after the last query
end_time = datetime.datetime.now()

# Output results
print("Query 5")
print(f"Total execution time: {(end_time - start_time).microseconds} ms")
print(f"Album terlaris adalah '{best_selling_album.title}' dengan ID: {best_selling_album.id} "
      f"dan total penjualan {album_sales[best_selling_album_id]} kopi.")
