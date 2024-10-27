from cassandra.cluster import Cluster
import csv
from datetime import datetime
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Helper function to parse date strings
def parse_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None

# Helper function to safely convert a string to an integer, using 0 if empty
def safe_int(value):
    return int(value) if value else 0

# Load transaction_albums data and organize by album_id as a map<int, int>
def load_transaction_albums(filepath):
    transaction_map = defaultdict(dict)  # Defaultdict with dict to store map<int, int>
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            album_id = safe_int(row['album_id'])
            transaction_id = safe_int(row['transaction_id'])
            quantity = safe_int(row['quantity'])
            # Store each transaction_id and quantity as a map for the album_id
            transaction_map[album_id][transaction_id] = quantity
    return transaction_map

# Load songs data and organize by album_id
def load_songs(filepath):
    songs_map = defaultdict(list)
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            album_id = safe_int(row['album_id'])
            song_id = safe_int(row['id'])
            songs_map[album_id].append(song_id)
    return songs_map

# Load group names from groups.csv and organize by group_id
def load_groups(filepath):
    group_map = {}
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            group_id = safe_int(row['id'])
            group_name = row['name']
            group_map[group_id] = group_name
    return group_map

# Seed albums table with combined data
def seed_albums(albums_filepath, transaction_albums_filepath, songs_filepath, groups_filepath):
    # Load associated data from other CSV files
    transaction_map = load_transaction_albums(transaction_albums_filepath)
    songs_map = load_songs(songs_filepath)
    group_map = load_groups(groups_filepath)

    # Read and insert albums data
    with open(albums_filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Parse individual fields from the albums CSV row
            album_id = safe_int(row['id'])
            title = row['title']
            release_date = parse_date(row['release_date'])
            album_type = row['type']
            duration = safe_int(row['duration'])
            genre = row['genre']
            group_id = safe_int(row['group_id'])
            stock = safe_int(row['stock'])
            price = safe_int(row['price'])
            group_name = group_map.get(group_id, "Unknown")  # Look up group_name by group_id

            # Get associated transaction_albums as a map and songs as a list
            transaction_albums = transaction_map.get(album_id, {})
            songs = songs_map.get(album_id, [])

            # Construct the insert query
            query = """
            INSERT INTO albums (id, title, release_date, type, duration, genre, stock, price, group_id, group_name, transaction_albums, songs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query with combined data
            session.execute(query, (
                album_id, title, release_date, album_type, duration, genre, stock, price, group_id, group_name, transaction_albums, songs
            ))

    print("Data for 'albums' table has been seeded successfully.")

# File paths for each CSV
albums_filepath = 'cleaned/albums.csv'
transaction_albums_filepath = 'cleaned/transaction_albums.csv'
songs_filepath = 'cleaned/songs.csv'
groups_filepath = 'cleaned/groups.csv'

# Run the seeding function
seed_albums(albums_filepath, transaction_albums_filepath, songs_filepath, groups_filepath)
