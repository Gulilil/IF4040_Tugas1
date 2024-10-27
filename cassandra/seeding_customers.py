from cassandra.cluster import Cluster
import csv
from datetime import datetime
from collections import defaultdict

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Helper function to safely convert a string to an integer, using 0 if empty
def safe_int(value):
    return int(value) if value else 0

# Load transactions data and organize by customer_id
def load_transactions(filepath):
    transactions_map = defaultdict(list)
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            customer_id = safe_int(row['customer_id'])
            transaction_id = safe_int(row['id'])
            transactions_map[customer_id].append(transaction_id)  # Add transaction ID to the customer
    return transactions_map

# Seed customers table with combined data
def seed_customers(customers_filepath, transactions_filepath):
    # Load associated transactions data
    transactions_map = load_transactions(transactions_filepath)

    # Read and insert customers data
    with open(customers_filepath, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Parse individual fields from the customers CSV row
            customer_id = safe_int(row['id'])
            name = row['name']
            email = row['email']
            username = row['username']
            password = row['password']
            address = row['address']
            country_id = safe_int(row['country_id'])

            # Get associated transactions as a list of transaction IDs
            transactions = transactions_map.get(customer_id, [])

            # Construct the insert query
            query = """
            INSERT INTO customers (id, name, email, username, password, address, country_id, transactions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query with combined data
            session.execute(query, (
                customer_id, name, email, username, password, address, country_id, transactions
            ))

    print("Data for 'customers' table has been seeded successfully.")

# File paths for each CSV
customers_filepath = 'cleaned/customers.csv'
transactions_filepath = 'cleaned/transactions.csv'

# Run the seeding function
seed_customers(customers_filepath, transactions_filepath)
