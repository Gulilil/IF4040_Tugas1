import couchdb
from datetime import datetime, timedelta

# Connect to CouchDB server
couch = couchdb.Server('http://localhost:5984/')  # Replace with your CouchDB URL if it's different
db = couch['your_database']  # Replace 'your_database' with your actual database name


# Function for No. 8: Delete cancelled transactions older than 1 year
def delete_old_cancelled_transactions():
    print("Deleting cancelled transactions older than 1 year...")
    
    # Get the date for 1 year ago
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    # Mango query to find all cancelled transactions older than 1 year
    query = {
        "selector": {
            "status": "cancelled",
            "transaction_date": {
                "$lt": one_year_ago  # Compare dates to find transactions older than 1 year
            }
        }
    }

    transactions = db.find(query)

    # Delete each transaction
    for transaction in transactions:
        db.delete(transaction)
        print(f"Deleted transaction {transaction['_id']} dated {transaction['transaction_date']}")


# Function for No. 9: Delete idols with no albums
def delete_idols_without_albums():
    print("Deleting idols with no albums...")

    # MapReduce view to find groups with albums (grouped by group_id)
    groups_with_albums = set()
    for row in db.view('idols_with_albums/idols_with_albums'):
        groups_with_albums.add(row.key)

    # Mango query to get all idols
    idols = db.find({"selector": {"type": "idol"}})

    # Delete idols that are not in groups with albums
    for idol in idols:
        if idol['group_id'] not in groups_with_albums:
            db.delete(idol)
            print(f"Deleted idol {idol['stage_name']} from group {idol['group_id']}")


# Main function to run the queries
if __name__ == "__main__":
    # No. 8: Delete cancelled transactions older than 1 year
    delete_old_cancelled_transactions()

    # No. 9: Delete idols with no albums
    delete_idols_without_albums()
