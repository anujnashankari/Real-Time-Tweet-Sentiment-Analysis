from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, regexp_replace, udf, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from textblob import TextBlob
import os
import sys
import time

# Sentiment functions
def get_polarity(text):
    if text is None:
        return 0.0
    return TextBlob(text).sentiment.polarity

def get_subjectivity(text):
    if text is None:
        return 0.0
    return TextBlob(text).sentiment.subjectivity

def get_sentiment_label(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

# Database connection properties
DB_URL = "jdbc:mysql://localhost:3306/tweetdb"
DB_USER = "sparkuser"
DB_PASS = "Spark@123"
DB_TABLE = "tweet_sentiments"
DB_DRIVER = "com.mysql.cj.jdbc.Driver"

try:
    # Register UDFs
    polarity_udf = udf(get_polarity, FloatType())
    subjectivity_udf = udf(get_subjectivity, FloatType())
    sentiment_label_udf = udf(get_sentiment_label, StringType())

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("KafkaTweetSentimentToMySQL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # More comprehensive schema for Kafka JSON messages
    schema = StructType([
        StructField("text", StringType()),
        StructField("timestamp", StringType()),
        StructField("user", StringType()),
        StructField("location", StringType())
    ])

    # Read from Kafka and parse JSON
    print("Starting Spark Streaming from Kafka...")
    try:
        stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "raw_tweets") \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select(
                col("data.text").alias("tweet"),
                col("data.timestamp").alias("timestamp"),
                col("data.user").alias("user"),
                col("data.location").alias("location")
            )

        # Handle null values
        validated_df = stream_df.filter(col("tweet").isNotNull())

        # Clean and prepare text
        cleaned = validated_df.withColumn("cleaned_text", 
                                       regexp_replace("tweet", r"http\S+|@\S+|#|\n", ""))

        # Apply sentiment analysis
        scored = cleaned.withColumn("polarity", polarity_udf(col("cleaned_text"))) \
                        .withColumn("subjectivity", subjectivity_udf(col("cleaned_text"))) \
                        .withColumn("sentiment", sentiment_label_udf(col("polarity")))

        # Function to write each micro-batch to MySQL
        def write_to_mysql(batch_df, batch_id):
            if not batch_df.isEmpty():
                count = batch_df.count()
                print(f"Writing batch {batch_id} with {count} tweets to MySQL")
                try:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", DB_URL) \
                        .option("driver", DB_DRIVER) \
                        .option("dbtable", DB_TABLE) \
                        .option("user", DB_USER) \
                        .option("password", DB_PASS) \
                        .mode("append") \
                        .save()
                except Exception as e:
                    print(f"Error writing to database: {e}")
            else:
                print(f"Batch {batch_id} is empty, skipping")

        # Write stream to MySQL
        query = scored.writeStream \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(write_to_mysql) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        print("Streaming pipeline started. Ctrl+C to stop.")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStreaming interrupted. Shutting down...")
        if 'query' in locals() and query is not None:
            query.stop()
        spark.stop()
        sys.exit(0)
        
except Exception as e:
    print(f"Error: {e}")
    print("Make sure Kafka and MySQL are running")
    sys.exit(1)