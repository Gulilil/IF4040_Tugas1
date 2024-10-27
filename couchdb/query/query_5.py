import requests
import time

# CouchDB connection details
username = 'admin'
password = 'admin'
db_name = 'transactions'
view_name = 'best_selling_albums'

# Function to get the best selling album
def get_best_selling_album():
    start_time = time.time()  # Start timing

    # Query the CouchDB view
    url = f'http://localhost:5984/{db_name}/_design/albums/_view/{view_name}?reduce=true&group=true'
    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        data = response.json()
        # Sort the rows by value in descending order
        sorted_albums = sorted(data['rows'], key=lambda x: x['value'], reverse=True)

        # Get the top album
        if sorted_albums:
            top_album = sorted_albums[0]
            print(f"Top Album ID: {top_album['key']}, Quantity Sold: {top_album['value']}")
        else:
            print("No albums found.")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

    end_time = time.time()  # End timing
    print(f"Time taken: {end_time - start_time:.4f} seconds")

# Call the function
get_best_selling_album()