import datetime
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust IP if necessary
session = cluster.connect("pedal")

# Start timing
start_time = datetime.datetime.now()

# Step 1: Get albums with genre 'Pop' (using ALLOW FILTERING for non-primary key filter)
albums_to_update = session.execute("""
    SELECT id, songs FROM albums
    WHERE genre = 'Pop' ALLOW FILTERING;
""")

# Step 2: Calculate the total duration for each album and update it
for album in albums_to_update:
    album_id = album.id
    song_ids = album.songs

    # Initialize total_duration
    total_duration = 0

    # Check if song_ids is not None before iterating
    if song_ids:
        for song_id in song_ids:
            song = session.execute("""
                SELECT duration FROM songs WHERE id = %s;
            """, (song_id,)).one()
            
            if song:
                total_duration += song.duration

    print("total_duration: ", total_duration)
    # Update the album duration
    session.execute("""
        UPDATE albums SET duration = %s WHERE id = %s;
    """, (total_duration, album_id))

# End timing
end_time = datetime.datetime.now()
duration = end_time - start_time

# Output total execution time in milliseconds and microseconds
print("Query M2 Durasi total album berhasil diperbarui.")
print(f"Total execution time: ({duration.microseconds} microseconds)")
