from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Nama grup yang dicari dan rentang harga
group_name = "Stray Kids"
min_price = 0
max_price = 500000

# Step 1: Cari ID grup berdasarkan nama grup
group_id_row = session.execute(f"""
    SELECT id FROM groups WHERE name = '{group_name}';
""").one()

if group_id_row:
    group_id = group_id_row.id
    
    # Step 2: Ambil data album untuk group_id tertentu dalam rentang harga
    album_rows = session.execute(f"""
        SELECT id, title, price
        FROM albums
        WHERE group_id = {group_id} AND price >= {min_price} AND price <= {max_price};
    """)
    
    # Step 3: Urutkan album berdasarkan harga
    albums = sorted(album_rows, key=lambda album: album.price)
    
    # Step 4: Tampilkan hasil
    print(f"Album dari grup '{group_name}' dalam rentang harga {min_price} - {max_price}, terurut dari harga terendah:")
    for album in albums:
        print(f"ID Album: {album.id}, Judul: {album.title}, Harga: {album.price}")
else:
    print(f"Grup dengan nama '{group_name}' tidak ditemukan.")
