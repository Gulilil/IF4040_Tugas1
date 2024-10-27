from cassandra.cluster import Cluster
import csv
from datetime import datetime
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Helper function to parse date strings with time
def parse_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        # If only the date is provided, format directly
        date = datetime.strptime(date_str, '%Y-%m-%d')
    
    return date.strftime('%Y-%m-%d')

# Helper function to safely convert a string to an integer, using 0 if empty
def safe_int(value):
    return int(value) if value else 0

# Load album details into a dictionary
def load_albums(filepath):
    album_map = {}
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            album_id = safe_int(row['id'])
            album_map[album_id] = {
                'name': row['title'],
                'price': safe_int(row['price'])
            }
    return album_map

# Load transaction album quantities and organize by transaction_id
def load_transaction_albums(filepath):
    transaction_album_map = defaultdict(dict)
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            transaction_id = safe_int(row['transaction_id'])
            album_id = safe_int(row['album_id'])
            quantity = safe_int(row['quantity'])
            transaction_album_map[transaction_id][album_id] = quantity
    return transaction_album_map

# Seed transactions table with combined data, with a limit of 50,000 rows
def seed_transactions(transactions_filepath, albums_filepath, transaction_albums_filepath):
    # Load associated data from other CSV files
    album_map = load_albums(albums_filepath)
    transaction_album_map = load_transaction_albums(transaction_albums_filepath)

    # Initialize a counter to limit the number of rows inserted
    row_count = 0

    # Read and insert transactions data
    with open(transactions_filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Parse individual fields from the transactions CSV row
            transaction_id = safe_int(row['id'])
            transaction_date = parse_date(row['transaction_date'])
            status = row['status']
            customer_id = safe_int(row['customer_id'])

            # Construct the transaction_albums map for this transaction
            transaction_albums = {}
            for album_id, quantity in transaction_album_map.get(transaction_id, {}).items():
                album = album_map.get(album_id, {})
                album_name = album.get('name', 'Unknown')
                album_price = album.get('price', 0)
                # Create a JSON-like string for each album entry
                transaction_albums[album_id] = f"{quantity},{album_name},{album_price}"

            # Construct the insert query
            query = """
            INSERT INTO transactions (id, transaction_date, status, customer_id, transaction_albums)
            VALUES (%s, %s, %s, %s, %s)
            """

            # Execute the query with combined data
            session.execute(query, (
                transaction_id, transaction_date, status, customer_id, transaction_albums
            ))

            # Increment the row counter
            row_count += 1
            print(row_count)

    print(f"Data for 'transactions' table has been seeded successfully. Total rows inserted: {row_count}")

# File paths for each CSV
transactions_filepath = 'cleaned/transactions.csv'
albums_filepath = 'cleaned/albums.csv'
transaction_albums_filepath = 'cleaned/transaction_albums.csv'

# Run the seeding function with the 50,000 row limit
seed_transactions(transactions_filepath, albums_filepath, transaction_albums_filepath)
