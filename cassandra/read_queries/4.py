import datetime
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Name of the group to search for
group_name = "BABYMONSTER"

# Step 1: Start timing before the first query
start_time = datetime.datetime.now()

# Retrieve group ID based on group name
group_id_row = session.execute(f"""
    SELECT id FROM groups WHERE name = '{group_name}' ALLOW FILTERING;
""").one()

# Step 2: Check if the group exists
if group_id_row:
    group_id = group_id_row.id
    
    # Step 3: Retrieve idol data associated with the group ID
    idol_rows = session.execute(f"""
        SELECT id, stage_name, full_name, date_of_birth, birthplace, gender, weight, height
        FROM idols
        WHERE group_id = {group_id} ALLOW FILTERING;
    """)
    
    # Step 4: End timing after the second query
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    print("Query 4")
    print(f"\nExecution time (in microseconds): {duration.microseconds}")
    # Step 5: Display results and query execution time
    print(f"Daftar idol dari grup '{group_name}':")
    for idol in idol_rows:
        print(f"ID: {idol.id}, Stage Name: {idol.stage_name}, Full Name: {idol.full_name}, "
              f"Date of Birth: {idol.date_of_birth}, Birthplace: {idol.birthplace}, "
              f"Gender: {idol.gender}, Weight: {idol.weight}, Height: {idol.height}")
    
else:
    print(f"Grup dengan nama '{group_name}' tidak ditemukan.")
