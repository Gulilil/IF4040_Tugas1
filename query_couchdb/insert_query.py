import couchdb

# Connect to CouchDB server
couch = couchdb.Server('http://localhost:5984/')  # Replace with your CouchDB URL if it's different
db = couch['your_database']  # Replace 'your_database' with your actual database name


# Function for No. 10: Insert a new transaction with multiple albums purchased
def insert_new_transaction(transaction_date, customer_id, albums_purchased):
    print(f"Inserting a new transaction for customer {customer_id} on {transaction_date}")

    # Create the transaction document
    transaction = {
        "type": "transaction",
        "transaction_date": transaction_date,
        "status": "Completed",
        "customer_id": customer_id,
        "albums": albums_purchased  # Array of albums purchased, each with album_id and quantity
    }

    # Insert the transaction document into the database
    db.save(transaction)
    print("Transaction inserted successfully.")
    print(f"Transaction details: {transaction}")


# Main function to run the insertion
if __name__ == "__main__":
    # Define the albums purchased in the transaction
    new_transaction_albums = [
        {"album_id": "album_001", "quantity": 2},
        {"album_id": "album_002", "quantity": 1}
    ]

    # Insert the new transaction (replace with your actual transaction date and customer ID)
    insert_new_transaction(transaction_date="2024-02-01", customer_id="customer_001", albums_purchased=new_transaction_albums)
