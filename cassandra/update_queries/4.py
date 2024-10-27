from cassandra.cluster import Cluster
import datetime

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

start = datetime.datetime.now()
# Query to select idols who do not belong to any group (group_id is NULL) using ALLOW FILTERING
idols_to_delete = session.execute("""
    SELECT id FROM idols
    WHERE group_id = 0 ALLOW FILTERING;
""")

# Delete each idol based on the retrieved IDs
for idol in idols_to_delete:
    print("Deleting idol with ID:", idol.id)
    idol_id = idol.id
    session.execute("""
        DELETE FROM idols WHERE id = %s;
    """, (idol_id,))

end_time = datetime.datetime.now()
print("Query M4")
print(f" Total execution time: {(end_time - start).microseconds} microseconds")
print("Idol tanpa grup berhasil dihapus.")
