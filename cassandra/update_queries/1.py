from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Step 1: Pilih semua album yang memenuhi kriteria genre 'Pop' dan release_date < '2020-01-01'
albums_to_update = session.execute("""
    SELECT id, price FROM albums
    WHERE genre = 'Pop' AND release_date < '2020-01-01';
""")

# Step 2: Perbarui harga setiap album
for album in albums_to_update:
    new_price = album.price * 1.10  # Menaikkan harga 10%
    session.execute(f"""
        UPDATE albums
        SET price = {new_price}
        WHERE id = {album.id};
    """)

print("Harga album berhasil diperbarui.")
