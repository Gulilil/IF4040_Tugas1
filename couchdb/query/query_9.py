import requests
import json
import time

# Start timing the entire process
start_time = time.time()

# Step 1: Fetch all documents with an empty group
fetch_start_time = time.time()
response = requests.post(
    "http://admin:admin@localhost:5984/idols/_find",
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "selector": {
            "group": {}
        },
        "fields": ["_id", "_rev"]
    })
)

# Check if the fetch was successful
if response.status_code == 200 or response.status_code == 201:
    idols = response.json().get('docs', [])
else:
    print(f"Failed to fetch idols: {response.status_code} - {response.text}")
    exit()

fetch_end_time = time.time()
fetch_duration = fetch_end_time - fetch_start_time
print(f"Fetched {len(idols)} idols with empty group in {fetch_duration:.2f} seconds.")

# Step 2: Delete the fetched documents
delete_start_time = time.time()

for idol in idols:
    delete_response = requests.delete(f"http://admin:admin@localhost:5984/idols/{idol['_id']}?rev={idol['_rev']}")
    
    if delete_response.status_code == 200 or delete_response.status_code == 201:
        print(f"Deleted idol ID {idol['_id']}")
    else:
        print(f"Failed to delete idol ID {idol['_id']}: {delete_response.status_code} - {delete_response.text}")

delete_end_time = time.time()
delete_duration = delete_end_time - delete_start_time
print(f"Deleted {len(idols)} idols in {delete_duration:.2f} seconds.")

# Total execution time
total_end_time = time.time()
total_duration = total_end_time - start_time
print(f"Total execution time: {total_duration:.2f} seconds.")