import datetime
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Group name and price range
group_name = "Stray Kids"
min_price = 0
max_price = 500000

# Step 1: Find group ID by group name
start_time = datetime.datetime.now()  # Start timing
group_id_row = session.execute(f"""
    SELECT id FROM groups WHERE name = '{group_name}'  ALLOW FILTERING;
""").one()

if group_id_row:
    group_id = group_id_row.id
    
    # Step 2: Retrieve album data for the specific group_id within the price range
    album_rows = session.execute(f"""
        SELECT id, title, price
        FROM albums
        WHERE group_id = {group_id} AND price >= {min_price} AND price <= {max_price} ALLOW FILTERING;
    """)
    end_time = datetime.datetime.now() # End timing
    print(f"Query 3 to retrieve albums executed in {(end_time - start_time).microseconds} ms")

    # Step 3: Sort albums by price
    albums = sorted(album_rows, key=lambda album: album.price)

    # Step 4: Display results
    print(f"Albums from group '{group_name}' in the price range {min_price} - {max_price}, sorted by lowest price:")
    for album in albums:
        print(f"Album ID: {album.id}, Title: {album.title}, Price: {album.price}")
else:
    print(f"Group with name '{group_name}' not found.")
