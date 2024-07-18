import csv
import time
import sql_functions as crud

connection = crud.connect_to_mysql() 

def data(row):
    # connection = crud.connect_to_mysql
    data = (
        row['track_name'],  # VARCHAR(255)
        row['artist(s)_name'],  # VARCHAR(255)
        row['artist_count'],  # INT
        row['released_year'],  # INT
        row['released_month'],  # INT
        row['released_day'],  # INT
        row['in_spotify_playlists'],  # INT
        row['in_spotify_charts'],  # INT
        row['streams'],  # BIGINT
        row['in_apple_playlists'],  # INT
        row['in_apple_charts'],  # INT
        row['in_deezer_playlists'],  # INT
        row['in_deezer_charts'],  # INT
        row['in_shazam_charts'],  # INT
        row['bpm'],  # INT
        row['key'],  # VARCHAR(255)
        row['mode'],  # VARCHAR(255)
        row['danceability_%'],  # INT
        row['valence_%'],  # INT
        row['energy_%'],  # INT
        row['acousticness_%'],  # INT
        row['instrumentalness_%'],  # INT
        row['liveness_%'],  # INT
        row['speechiness_%'],  # INT
        row['topic']
    )
    print(data)
    crud.insert_row(connection, data)
