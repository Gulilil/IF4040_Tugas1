from cassandra.cluster import Cluster
from cassandra.util import Frozen

import csv

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Change IP if necessary
session = cluster.connect("pedal")

# Ensure transaction_albums is a list of tuples
transaction_albums = [Frozen((1001, 2))]  # Correct structure
query = """
            INSERT INTO albums (id, title, release_date, type, duration, genre, stock, price, group_id, group_name, transaction_albums, songs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
# Your other data values for the insert
data = (
    1,                          # id
    "Best Hits",                # title
    "2022-07-15",               # release_date
    "Album",                    # type
    3600,                       # duration
    "Pop",                      # genre
    500,                        # stock
    1500,                       # price
    101,                        # group_id
    "K-Pop Group",              # group_name
    transaction_albums,         # transaction_albums
    [101, 102, 103]             # songs
)

# Execute the insert with the query and data
session.execute(query, data)
