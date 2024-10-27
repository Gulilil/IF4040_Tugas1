from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)  # Sesuaikan IP jika perlu
session = cluster.connect("pedal")

# Step 1: Get albums with genre 'Pop'
albums_to_update = session.execute("""
    SELECT id, songs FROM albums
    WHERE genre = 'Pop';
""")

# Step 2: Calculate the total duration for each album and update it
for album in albums_to_update:
    album_id = album.id
    song_ids = album.songs

    # Get the duration of each song in the album
    total_duration = 0
    for song_id in song_ids:
        song = session.execute("""
            SELECT duration FROM songs WHERE id = %s;
        """, (song_id,)).one()
        
        if song:
            total_duration += song.duration

    # Update the album duration
    session.execute("""
        UPDATE albums SET duration = %s WHERE id = %s;
    """, (total_duration, album_id))

print("Durasi total album berhasil diperbarui.")
