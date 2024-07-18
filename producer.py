import csv
import time
# import threading
from kafka import KafkaProducer
import data_to_sql as sql_data
import sql_functions as crud
import datetime

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

connection = crud.connect_to_mysql()
connection2 = crud.connect_to_mysql()
connection3 = crud.connect_to_mysql()

# CSV file path
csv_file = 'spotify_data.csv'

# Function to publish data to Kafka topics
def publish_data(topic, data):
    producer.send(topic, data.encode('utf-8'))
    producer.flush()

# Read CSV file and publish data to Kafka topics
try:
    current_time = datetime.datetime.now()
    # Extract only the time part
    time_str = current_time.strftime('%H:%M:%S')
    with open(csv_file, mode='r', encoding='latin-1') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            topic = row.get('topic')
            if topic == 'search_artist':
                artist_names = f"{row.get('artist(s)_name')} - {row.get('released_month')}"
                print(topic, "->", artist_names)
                if artist_names:
                    publish_data('search_artist', artist_names)
            elif topic == 'popular_artist':
                artist_names_year = f"{row.get('artist(s)_name')} - {row.get('released_year')}"
                print(topic, "->", artist_names_year)
                if artist_names_year:
                    publish_data('popular_artist', artist_names_year)
            elif topic == 'streams':
                streams = row.get('streams')
                print(topic, "->", streams)
                if streams:
                    publish_data('streams', streams)
            # time.sleep(0.37)
            sql_data.data(row)
    end_time = datetime.datetime.now()
    # Subtract the times and get the difference
    time_difference = end_time - current_time
    print("---------------------------------------------------------------------------")
    print("---------------------Batch Processing--------------------------------------")
    print("---------------------------------------------------------------------------")
    print("Time Taken for inserting into database:", time_difference.total_seconds())
    consumer2 = crud.consumer2(connection,'2020',time_difference.total_seconds())
    consumer1 = crud.consumer1(connection2, 'Taylor Swift',time_difference.total_seconds())
    consumer3 = crud.consumer3(connection3,'100000000',time_difference.total_seconds())

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    producer.close()

print("Producer finished execution.")
