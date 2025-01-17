from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Popular Artist Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic name
kafka_topic_name = 'popular_artist'

# Define the schema for the incoming data
# schema = "value STRING"

# Define the streaming DataFrame using Kafka as the source
stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load() \
    .withColumn("value", col("value").cast("string")) \
    .selectExpr("CAST(value AS STRING)")

# Define a function to calculate and print the time taken for processing each batch
def process_batch(df, epoch_id):
    current_time = datetime.datetime.now()
    time_difference = current_time - start_time
    print("Batch ID:", epoch_id, "Time Taken:", time_difference.total_seconds(), "seconds")
    # You can perform additional processing or write to an external sink here
    df.show()

# Define the processing logic
processed_df = stream_df \
    .withColumn("artist_name", split(col("value"), " - ")[0]) \
    .withColumn("year_released", split(col("value"), " - ")[1].cast("int")) \
    .filter(col("year_released") >= 2020) \
    .groupBy("artist_name") \
    .count() \
    .orderBy(desc("count"))

start_time = datetime.datetime.now()  # Start time for the overall processing

# Start the streaming query to process the incoming streams
query = processed_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .start()
# Wait for the query to terminate
query.awaitTermination()