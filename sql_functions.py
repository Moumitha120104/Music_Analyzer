import mysql.connector
import datetime
import time
def connect_to_mysql():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Moumitha04!",
        )
        # print("Connected to MySQL successfully!")

        # Create the database if it doesn't exist
        cursor = connection.cursor()
        cursor.execute("DROP DATABASE IF EXISTS spotify_songs")
        cursor.execute("CREATE DATABASE IF NOT EXISTS spotify_songs")
        cursor.close()

        # Select the database
        connection.database = "spotify_songs"
        create_table(connection)
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_table(connection):
    try:
        if connection.is_connected():
            # Create table if it doesn't exist
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streamingdata (
                track_name VARCHAR(255),
                artist_name VARCHAR(255),
                artist_count INT,
                released_year INT,
                released_month INT,
                released_day INT,
                in_spotify_playlists VARCHAR(255),
                in_spotify_charts VARCHAR(255),
                streams VARCHAR(500),
                in_apple_playlists VARCHAR(255),
                in_apple_charts VARCHAR(255),
                in_deezer_playlists VARCHAR(255),
                in_deezer_charts VARCHAR(255),
                in_shazam_charts VARCHAR(255),
                bpm VARCHAR(255),
                key_col VARCHAR(255),
                mode_col VARCHAR(255),
                danceability VARCHAR(255),
                valence VARCHAR(255),
                energy VARCHAR(255),
                acousticness VARCHAR(255),
                instrumentalness VARCHAR(255),
                liveness VARCHAR(255),
                speechiness VARCHAR(255),
                topic VARCHAR(255)
            );
            """)
            # print("Table created successfully!")
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")

def insert_row(connection, data):
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            query = """
                INSERT INTO streamingdata 
                (track_name, artist_name, artist_count, released_year, released_month, released_day, in_spotify_playlists, in_spotify_charts, streams, in_apple_playlists, in_apple_charts, in_deezer_playlists, in_deezer_charts, in_shazam_charts, bpm, key_col, mode_col, danceability, valence, energy, acousticness, instrumentalness, liveness, speechiness, topic) 
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(query, data)
            connection.commit()
            # print("Row inserted successfully!")
    except mysql.connector.Error as e:
        print(f"Error inserting row: {e}")


def consumer1(connection, artist_input, insertion_time):
    try:
        current_time = datetime.datetime.now()
        # Extract only the time part
        time_str = current_time.strftime('%H:%M:%S')
        if connection.is_connected():
            cursor = connection.cursor()
            query = """
                SELECT
                    artist_name,
                    released_month,
                    COUNT(*) AS count
                FROM
                    streamingdata
                WHERE
                    artist_name = %s and topic = 'search_artist'
                GROUP BY
                    artist_name,
                    released_month
                ORDER BY
                    released_month;"""
            cursor.execute(query, (artist_input,))
            # connection.commit()
            end_time = datetime.datetime.now()
            # Subtract the times and get the difference
            time_difference = end_time - current_time
            print("Time Taken for batch execution consumer-1:", time_difference.total_seconds() + insertion_time)
    except mysql.connector.Error as e:
        print(f"Error inserting row: {e}")

def consumer2(connection, current_year, insertion_time):
    try:
        current_time = datetime.datetime.now()
        # Extract only the time part
        time_str = current_time.strftime('%H:%M:%S')
        if connection.is_connected():
            cursor = connection.cursor()
            query = """
                select artist_name,count(released_year) as count
                from streamingdata 
                where topic="popular_artist" and released_year >= %s
                group by artist_name
                order by count desc;"""
            cursor.execute(query, (current_year,))
            # connection.commit()
            end_time = datetime.datetime.now()
            # Subtract the times and get the difference
            time_difference = end_time - current_time
            print("Time Taken for batch execution consumer-2:", time_difference.total_seconds() + insertion_time)
    except mysql.connector.Error as e:
        print(f"Error inserting row: {e}")

def consumer3(connection, threshold, insertion_time):
    try:
        current_time = datetime.datetime.now()
        # Extract only the time part
        time_str = current_time.strftime('%H:%M:%S')
        if connection.is_connected():
            cursor = connection.cursor()
            query = """
               SELECT COUNT(*) AS streams_count
                FROM streamingdata
                WHERE streams > %s and topic = 'streams';"""
            cursor.execute(query, (threshold,))
            # connection.commit()
            end_time = datetime.datetime.now()
            # Subtract the times and get the difference
            time_difference = end_time - current_time
            print("Time Taken for batch execution consumer-3:", time_difference.total_seconds() + insertion_time)
    except mysql.connector.Error as e:
        print(f"Error inserting row: {e}")

def close_connection(connection):
    if connection.is_connected():
        connection.close()
        print("Connection to MySQL closed.")
