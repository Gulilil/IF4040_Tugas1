from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Nama grup yang dicari
group_name = "BABYMONSTER"

# Step 1: Cari ID grup berdasarkan nama grup
group_id_row = session.execute(f"""
    SELECT id FROM groups WHERE name = '{group_name}';
""").one()

if group_id_row:
    group_id = group_id_row.id
    
    # Step 2: Ambil data idol yang sesuai dengan group_id
    idol_rows = session.execute(f"""
        SELECT id, stage_name, full_name, date_of_birth, birthplace, gender, weight, height
        FROM idols
        WHERE group_id = {group_id};
    """)
    
    # Step 3: Tampilkan hasil
    print(f"Daftar idol dari grup '{group_name}':")
    for idol in idol_rows:
        print(f"ID: {idol.id}, Stage Name: {idol.stage_name}, Full Name: {idol.full_name}, "
              f"Date of Birth: {idol.date_of_birth}, Birthplace: {idol.birthplace}, "
              f"Gender: {idol.gender}, Weight: {idol.weight}, Height: {idol.height}")
else:
    print(f"Grup dengan nama '{group_name}' tidak ditemukan.")
