import requests
import json
import time

# Start timing the entire process
start_time = time.time()

# Step 1: Fetch all albums
fetch_start_time = time.time()
response = requests.post(
    "http://admin:admin@localhost:5984/albums/_find",
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "selector": {"genre": "Pop"},  # Fetch all albums
        "fields": ["_id", "_rev", "songs"],
    })
)

# Check if the fetch was successful
if response.status_code == 200 or response.status_code == 201:
    albums = response.json().get('docs', [])
else:
    print(f"Failed to fetch albums: {response.status_code} - {response.text}")
    exit()

fetch_end_time = time.time()
fetch_duration = fetch_end_time - fetch_start_time
print(f"Fetched {len(albums)} albums in {fetch_duration:.2f} seconds.")

# Step 2: Update duration
update_start_time = time.time()

for album in albums:
    # Fetch the complete document to preserve all attributes
    doc_response = requests.get(f"http://admin:admin@localhost:5984/albums/{album['_id']}")
    if doc_response.status_code == 200:
        full_album = doc_response.json()
        
        # Calculate total duration from songs
        total_duration = sum(song['duration'] for song in album.get('songs', []))
        
        # Add the new duration attribute
        full_album['duration'] = total_duration
        
        # # Print the full album data before uploading
        # print(f"Uploading album ID {full_album['_id']} with data: {json.dumps(full_album, indent=2)}")
        
        # Upload the updated album back to CouchDB
        update_response = requests.put(
            f"http://admin:admin@localhost:5984/albums/{full_album['_id']}",
            headers={"Content-Type": "application/json"},
            data=json.dumps(full_album)
        )

        if update_response.status_code == 200 or update_response.status_code == 201:
            print(f"Updated album ID {full_album['_id']} with data: {json.dumps(full_album, indent=2)}")
        else:
            print(f"Failed to update album ID {full_album['_id']}: {update_response.status_code} - {update_response.text}")

update_end_time = time.time()
update_duration = update_end_time - update_start_time
print(f"Updated {len(albums)} albums in {update_duration:.2f} seconds.")

# Total execution time
total_end_time = time.time()
total_duration = total_end_time - start_time
print(f"Total execution time: {total_duration:.2f} seconds.")
