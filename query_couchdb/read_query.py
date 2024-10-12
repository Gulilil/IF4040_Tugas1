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


# Function for No. 2: Get groups ordered by most albums sold using MapReduce
def get_groups_ordered_by_albums_sold():
    print("Fetching groups ordered by total albums sold...")
    
    # Query the view from the design document
    results = db.view('group_album_sales/album_sales_by_group', reduce=True, group=True, descending=True)

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


# Function for No. 5: Get the best-selling album in each genre using MapReduce
def get_best_selling_album_by_genre():
    print("Fetching the best-selling album in each genre...")
    
    # Query the view from the design document
    results = db.view('best_selling_album_by_genre/album_sales_by_genre', reduce=True, group_level=2)

    genre_sales = {}

    # Process results by genre
    for row in results:
        genre = row.key[0]
        album_title = row.key[1]
        total_sales = row.value

        # Check if this album has the highest sales for this genre
        if genre not in genre_sales or genre_sales[genre]['total_sales'] < total_sales:
            genre_sales[genre] = {'album_title': album_title, 'total_sales': total_sales}

    # Print the best-selling album for each genre
    for genre, data in genre_sales.items():
        print(f"Genre: {genre}, Best-Selling Album: {data['album_title']}, Total Sales: {data['total_sales']}")

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
