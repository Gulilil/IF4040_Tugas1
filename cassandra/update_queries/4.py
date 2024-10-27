from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Query to select idols who do not belong to any group (group_id is NULL)
idols_to_delete = session.execute("""
    SELECT id FROM idols
    WHERE group_id = NULL;
""")

# Delete each idol based on the retrieved IDs
for idol in idols_to_delete:
    idol_id = idol.id
    session.execute("""
        DELETE FROM idols WHERE id = %s;
    """, (idol_id,))

print("Idol tanpa grup berhasil dihapus.")
