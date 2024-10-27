from cassandra.cluster import Cluster
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Step 1: Ambil semua transaksi album
album_rows = session.execute("""
    SELECT id, title, transaction_albums FROM albums;
""")

# Step 2: Hitung total penjualan untuk setiap album
album_sales = defaultdict(int)

for album in album_rows:
    for transaction in album.transaction_albums:
        album_id, quantity = transaction
        album_sales[album_id] += quantity

# Step 3: Temukan album terlaris
best_selling_album_id = max(album_sales, key=album_sales.get)
best_selling_album = session.execute(f"""
    SELECT id, title FROM albums WHERE id = {best_selling_album_id};
""").one()

# Output hasil
print(f"Album terlaris adalah '{best_selling_album.title}' dengan ID: {best_selling_album.id} dan total penjualan {album_sales[best_selling_album_id]} kopi.")
