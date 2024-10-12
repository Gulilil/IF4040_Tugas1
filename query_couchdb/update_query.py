import couchdb

# Connect to CouchDB server
couch = couchdb.Server('http://localhost:5984/')  # Replace with your CouchDB URL if it's different
db = couch['your_database']  # Replace 'your_database' with your actual database name


# Function for No. 6: Update album prices by genre
def update_album_prices_by_genre(genre, price_increase_factor):
    print(f"Updating album prices for genre: {genre}")
    
    # Mango query to find all albums in the specified genre
    query = {
        "selector": {
            "genre": genre
        }
    }

    albums = db.find(query)

    # Update the price for each album
    for album in albums:
        original_price = album.get('price', 0)  # Get the original price, or default to 0
        new_price = original_price * price_increase_factor  # Apply the price increase factor
        album['price'] = new_price
        db.save(album)  # Save the updated album document back to CouchDB
        print(f"Updated price for album '{album['title']}' from {original_price} to {new_price}")


# Function for No. 7: Update album total duration based on the duration of its songs
def update_album_duration():
    print("Updating total duration for all albums based on song durations...")

    # Query all albums
    albums = db.find({"selector": {"type": "album"}})

    # Update total duration for each album
    for album in albums:
        if 'songs' in album:  # Check if there are songs embedded in the album
            total_duration = sum([song.get('duration', 0) for song in album['songs']])
            album['total_duration'] = total_duration  # Update the total duration field
            db.save(album)  # Save the updated album document back to CouchDB
            print(f"Updated total duration for album '{album['title']}' to {total_duration} seconds")


# Main function to run the queries
if __name__ == "__main__":
    # No. 6: Update album prices by genre
    update_album_prices_by_genre(genre="Rock", price_increase_factor=1.10)

    # No. 7: Update album duration based on song durations
    update_album_duration()
