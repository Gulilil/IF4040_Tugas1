from cassandra.cluster import Cluster
import csv
from datetime import datetime
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Helper function to parse date strings
def parse_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d') if date_str else None

# Helper function to safely convert a string to an integer, using 0 if empty
def safe_int(value):
    return int(value) if value else 0

# Load idols data and organize by group_id
def load_idols(filepath):
    idols_map = defaultdict(dict)
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            group_id = safe_int(row['group_id'])
            idol_id = safe_int(row['id'])
            stage_name = row['stage_name']
            full_name = row['full_name']
            country_id = safe_int(row['country_id'])
            gender = row['gender']
            # Create a JSON-like string for each idol entry
            idols_map[group_id][idol_id] = f"{stage_name},{full_name},{country_id},{gender}"
    return idols_map

# Load albums data and organize by group_id
def load_albums(filepath):
    albums_map = defaultdict(dict)
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            group_id = safe_int(row['group_id'])
            album_id = safe_int(row['id'])
            title = row['title']
            # Store album title for each album_id
            albums_map[group_id][album_id] = title
    return albums_map

# Seed groups table with combined data, with a limit of 50,000 rows
def seed_groups(groups_filepath, idols_filepath, albums_filepath, limit=50000):
    # Load associated data from other CSV files
    idols_map = load_idols(idols_filepath)
    albums_map = load_albums(albums_filepath)

    # Initialize a counter to limit the number of rows inserted
    row_count = 0

    # Read and insert groups data
    with open(groups_filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:

            # Parse individual fields from the groups CSV row
            group_id = safe_int(row['id'])
            name = row['name']
            debut = parse_date(row['debut'])
            company_id = safe_int(row['company_id'])
            fanclub_name = row['fanclub_name']
            active = row['active'].lower() == 'true'  # Convert active status to boolean
            group_type = row['type']

            # Get associated idols and albums as maps
            idols = idols_map.get(group_id, {})
            albums = albums_map.get(group_id, {})

            # Construct the insert query
            query = """
            INSERT INTO groups (id, name, debut, company_id, fanclub_name, active, type, idols, albums)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query with combined data
            session.execute(query, (
                group_id, name, debut, company_id, fanclub_name, active, group_type, idols, albums
            ))

            # Increment the row counter
            row_count += 1

    print(f"Data for 'groups' table has been seeded successfully. Total rows inserted: {row_count}")

# File paths for each CSV
groups_filepath = 'cleaned/groups.csv'
idols_filepath = 'cleaned/idols.csv'
albums_filepath = 'cleaned/albums.csv'

# Run the seeding function with the 50,000 row limit
seed_groups(groups_filepath, idols_filepath, albums_filepath, limit=50000)
