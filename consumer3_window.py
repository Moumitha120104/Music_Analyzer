from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime
import signal

# Kafka topic name
kafka_topic_name = "streams"

# Kafka bootstrap servers
kafka_bootstrap_servers = 'localhost:9092'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Stream Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Set the log level to ERROR to avoid excessive output
spark.sparkContext.setLogLevel("ERROR")

# Define the streaming DataFrame using Kafka as the source
streams_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()

# Cast the value column from binary to string and extract the stream count
streams_df = streams_df \
    .withColumn("value", col("value").cast("string")) \
    .selectExpr("CAST(value AS STRING) AS streams")

# Define a UDF to increment the count variable based on the stream count
@udf
def update_count(count, stream_count):
    if stream_count > 100000000:
        return count + 1
    else:
        return count

# Define a window specification for 2-minute windows
window_spec = Window.orderBy("timestamp").rangeBetween(-120, 0)

# Start time
start_time = datetime.datetime.now()

# Define a function to calculate and print the time taken for processing each batch
def process_batch(df, epoch_id):
    current_time = datetime.datetime.now()
    time_difference = current_time - start_time
    print("Batch ID:", epoch_id, "Time Taken:", time_difference.total_seconds(), "seconds")
    # You can perform additional processing or write to an external sink here
    df.show()

# Define a streaming query to process the incoming streams with the window
query = streams_df \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "2 minutes") \
    .withColumn("stream_count", col("streams").cast("long")) \
    .withColumn("count", lit(0)) \
    .withColumn("updated_count", update_count(col("count"), col("stream_count"))) \
    .groupBy(window("timestamp", "2 minutes")) \
    .agg(sum("updated_count").alias("total_count"), sum("stream_count").alias("total_streams")) \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the query to terminate
query.awaitTermination()
