from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import signal
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Search Artist Window Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic name
kafka_topic_name = 'search_artist'

# Define the streaming DataFrame using Kafka as the source
stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load() \
    .withColumn("value", col("value").cast("string"))

# Define the processing logic
ans = {}
batch = {}

def process_messages(batch_df, batch_id):
    global ans, batch
    
    # Extract artist name and released month
    batch_df = batch_df.withColumn("artist_name", split(col("value"), " - ")[0].cast("string")) \
                       .withColumn("month_released", split(col("value"), " - ")[1].cast("int"))
    
    # Update batch dictionary
    batch_data = batch_df.select("artist_name", "month_released").collect()
    for row in batch_data:
        artist_name = row["artist_name"]
        released_month = row["month_released"]
        if artist_name in batch:
            batch[artist_name].append(released_month)
        else:
            batch[artist_name] = [released_month]
    
    # Check if at least 10 messages are received for any artist
    if batch_df.count() >= 10:
        print(f"At least 10 messages received in batch {batch_id}. Triggering action...")
        # Perform aggregation for released months
        for artist, months in batch.items():
            if artist == 'Taylor Swift':
                for month in months:
                    if month in ans:
                        ans[month] += 1
                    else:
                        ans[month] = 1
        process_batch(batch_df, batch_id)
        # Clear the batch dictionary after processing
        batch.clear()


# Define a function to process each batch
def process_batch(df, epoch_id):
    # Your batch processing logic here
    current_time = datetime.datetime.now()
    time_difference = current_time - start_time
    print("Batch ID:", epoch_id, "Time Taken:", time_difference.total_seconds(), "seconds")
    df.show()

# Start time
start_time = datetime.datetime.now()

# Start the streaming query to process the incoming streams
query = stream_df \
    .writeStream \
    .foreachBatch(process_messages) \
    .outputMode("append") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
