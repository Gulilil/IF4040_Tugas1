from cassandra.cluster import Cluster
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika diperlukan
session = cluster.connect("pedal")

# Step 1: Ambil semua transaksi dan hitung pembelian setiap album
print("Mengambil data transaksi...")
transaction_rows = session.execute("""
    SELECT transaction_albums FROM transactions;
""")

# Inisialisasi dictionary untuk menyimpan total pembelian per album
album_purchase_count = defaultdict(int)

# Hitung jumlah pembelian per album dari data transaksi
for transaction in transaction_rows:
    if transaction.transaction_albums:
        for album_tuple in transaction.transaction_albums:
            album_id = album_tuple[0]  # ID album ada di posisi pertama tuple
            quantity = album_tuple[1]  # Jumlah pembelian di posisi kedua tuple
            album_purchase_count[album_id] += quantity

# Step 2: Ambil data grup dan daftar albumnya
print("Mengambil data grup dan albumnya...")
group_rows = session.execute("""
    SELECT id, name, albums FROM groups;
""")

# Inisialisasi dictionary untuk menyimpan total pembelian per grup
group_purchase_count = []

for group in group_rows:
    total_purchases = 0
    if group.albums:
        for album_tuple in group.albums:
            album_id = album_tuple[0]  # ID album ada di posisi pertama tuple
            total_purchases += album_purchase_count[album_id]  # Tambahkan pembelian album ini ke total grup

    # Tambahkan jumlah pembelian grup ke list
    group_purchase_count.append((group.name, total_purchases))

# Step 3: Urutkan grup berdasarkan jumlah pembelian dari terbesar ke terkecil
group_purchase_count.sort(key=lambda x: x[1], reverse=True)

# Step 4: Tampilkan hasil
print("Grup berdasarkan urutan jumlah pembelian albumnya:")
for group_name, total_purchases in group_purchase_count:
    print(f"Grup: {group_name}, Jumlah Pembelian Album: {total_purchases}")
