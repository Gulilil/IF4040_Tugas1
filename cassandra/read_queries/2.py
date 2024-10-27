import datetime
from cassandra.cluster import Cluster
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Step 1: Retrieve all transactions and calculate purchases per album
print("Mengambil data transaksi...")

# Start timing for Query 1
start = datetime.datetime.now()
transaction_rows = session.execute("""
    SELECT transaction_albums FROM transactions;
""")
# End timing for Query 1
end = datetime.datetime.now()
duration = end - start
print("Query 2.1 executed")
print(f"Execution time (in microseconds): {duration.microseconds}")

# Initialize dictionary to store total purchases per album
album_purchase_count = defaultdict(int)
start_res = datetime.datetime.now()
# Calculate purchase count per album from transaction data
for transaction in transaction_rows:
    if transaction.transaction_albums:
        for album_id, details in transaction.transaction_albums.items():
            # Split the details string to extract quantity
            quantity_str = details.split(',')[0]  # Extract the first part as quantity
            album_purchase_count[album_id] += int(quantity_str)  # Convert quantity to int


# Step 2: Retrieve group data and their album lists
print("Mengambil data grup dan albumnya...")

# Start timing for Query 2
start = datetime.datetime.now()
group_rows = session.execute("""
    SELECT id, name, albums FROM groups;
""")
# End timing for Query 2
end = datetime.datetime.now()
duration = end - start
print("Query 2.2 executed")
print(f"Execution time (in microseconds): {duration.microseconds}")

# Initialize a list to store total purchases per group
group_purchase_count = []

for group in group_rows:
    total_purchases = 0
    if group.albums:
        for album_id in group.albums:  # Assuming albums is a list of album IDs
            total_purchases += album_purchase_count.get(album_id, 0)  # Add this album's purchases to the group's total

    # Add the group's purchase count to the list
    group_purchase_count.append((group.name, total_purchases))

# Step 3: Sort groups by total purchases in descending order
group_purchase_count.sort(key=lambda x: x[1], reverse=True)
end_res = datetime.datetime.now()
duration_res = end_res - start_res

print(f"final Processing Executed: {duration_res.microseconds} ms")
# Step 4: Display results
# print("Grup berdasarkan urutan jumlah pembelian albumnya:")
# for group_name, total_purchases in group_purchase_count:
#     print(f"Grup: {group_name}, Jumlah Pembelian Album: {total_purchases}")
