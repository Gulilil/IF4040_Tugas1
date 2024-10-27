from cassandra.cluster import Cluster
import csv

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])  # Change IP if necessary
session = cluster.connect()

# Create keyspace (if not already created)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS pedal
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.set_keyspace('pedal')

# Create the 'companies' table
session.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        id INT PRIMARY KEY,
        name TEXT,
        headquarter TEXT,
        country_id INT,
        founded_year INT
    )
""")

# Create the 'groups' table
session.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        id INT PRIMARY KEY,
        name TEXT,
        debut DATE,
        company_id INT,
        fanclub_name TEXT,
        active BOOLEAN,
        type TEXT
    )
""")

# Function to insert data from CSV into Cassandra
def insert_data_from_csv(csv_file_path, table_name, columns):
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            values = [row[col] for col in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            session.execute(query, values)

# Example CSV file paths
companies_csv = 'data/companies.csv'
groups_csv = 'data/groups.csv'

# Insert data into companies and groups tables
insert_data_from_csv(companies_csv, 'companies', ['id', 'name', 'headquarter', 'country_id', 'founded_year'])
insert_data_from_csv(groups_csv, 'groups', ['id', 'name', 'debut', 'company_id', 'fanclub_name', 'active', 'type'])

print("Data inserted successfully.")
