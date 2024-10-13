import couchdb

# Connect to CouchDB server
couch = couchdb.Server('http://localhost:5984/')  # Replace with your CouchDB URL if it's different
db = couch['your_database']  # Replace 'your_database' with your actual database name


# Function for No. 1: Display transaction history for a specific user
def get_user_transaction_history(customer_name):
    print(f"Fetching transaction history for user: {customer_name}")
    
    # Mango query to get transactions for the specific customer
    query = {
        "selector": {
            "customer_name": customer_name
        },
        "fields": ["transaction_date", "albums.title", "albums.quantity"]
    }

    transactions = db.find(query)

    # Print out the transaction history
    for transaction in transactions:
        print(f"Transaction Date: {transaction['transaction_date']}")
        for album in transaction['albums']:
            print(f"  Album: {album['title']}, Quantity: {album['quantity']}")
        print("-" * 40)


def get_groups_ordered_by_albums_sold(limit=10):
    print(f"Fetching top {limit} groups ordered by total albums sold...")

    # Query the view with the stale=ok option to improve performance
    results = db.view('group_album_sales/album_sales_by_group', reduce=True, group=True, descending=True, limit=limit, stale='ok')

    # Print the results
    for row in results:
        group_name = row.key
        total_sales = row.value
        print(f"Group: {group_name}, Total Albums Sold: {total_sales}")

# Function for No. 3: Get the number of albums released by each group
def get_album_count_by_group():
    print("Fetching the number of albums released by each group...")
    
    # Query the view from the design document
    results = db.view('group_album_count/album_count_by_group', reduce=True, group=True)

    # Print the results
    for row in results:
        group_name = row.key
        album_count = row.value
        print(f"Group: {group_name}, Albums Released: {album_count}")

# Function for No. 4: Display all idols from a specific group
def get_idols_from_group(group_name):
    print(f"Fetching idols from group: {group_name}")
    
    # Mango query to get idols from a specific group
    query = {
        "selector": {
            "group_name": group_name
        },
        "fields": ["stage_name", "full_name", "date_of_birth", "birthplace"]
    }

    idols = db.find(query)

    # Print out the idols from the group
    for idol in idols:
        print(f"Stage Name: {idol['stage_name']}")
        print(f"Full Name: {idol['full_name']}")
        print(f"Date of Birth: {idol['date_of_birth']}")
        print(f"Birthplace: {idol['birthplace']}")
        print("-" * 40)


# Function for No. 5: Get the best-selling album ordered by total sales
def get_best_selling_albums():
    print("Fetching the best-selling albums...")

    # Query the MapReduce view
    results = db.view('album_sales/album_sales_total', reduce=True, group=True, descending=True)

    # Fetch album details from each album_id and print the total sales
    for row in results:
        album_id = row.key
        total_sales = row.value
        album_doc = db.get(album_id)  # Get album details by album_id
        album_title = album_doc.get('title', 'Unknown')
        print(f"Album: {album_title}, Total Sales: {total_sales}")

# Main function to run the queries
if __name__ == "__main__":
    # Replace 'Nama Pengguna' with the name of the user you want to search for in No. 1
    get_user_transaction_history("Nama Pengguna")
    
    # Get groups ordered by most albums sold (No. 2)
    get_groups_ordered_by_albums_sold()
    
    # Get the number of albums released by each group (No. 3)
    get_album_count_by_group()

    # Replace 'Nama Grup' with the name of the group you want to search for in No. 4
    get_idols_from_group("Nama Grup")
    
    # Get the best-selling album in each genre (No. 5)
    get_best_selling_album_by_genre()
