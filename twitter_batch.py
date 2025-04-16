from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col
import time
import sys

# Database connection properties
DB_URL = "jdbc:mysql://localhost:3306/tweetdb"
DB_USER = "sparkuser"
DB_PASS = "Spark@123"
DB_TABLE = "tweet_sentiments"
DB_DRIVER = "com.mysql.cj.jdbc.Driver"

try:
    print("Starting batch analysis of tweets...")
    start_time = time.time()

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TweetBatchSentiment") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read from MySQL database
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", DB_TABLE) \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", DB_DRIVER) \
            .load()
        
        # Show sample data
        print("\nSample data:")
        df.select("tweet", "sentiment", "polarity").show(5, truncate=True)
        
        # Get total count
        total_count = df.count()
        print(f"\nTotal tweets analyzed: {total_count}")

        # Perform sentiment aggregation
        agg_df = df.groupBy("sentiment") \
            .agg(
                count("*").alias("tweet_count"),
                avg("polarity").alias("average_polarity")
            ) \
            .orderBy(col("tweet_count").desc())

        # Show results in terminal
        print("\n=== BATCH MODE RESULTS ===")
        agg_df.show(truncate=False)
        
        # Additional insights
        if total_count > 0:
            sentiment_distribution = agg_df.select(
                col("sentiment"), 
                (col("tweet_count") / total_count * 100).alias("percentage")
            )
            
            print("\nSentiment Distribution (%):")
            sentiment_distribution.show(truncate=False)

        end_time = time.time()
        print(f"\nBatch Mode Execution Time: {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Database error: {e}")
        print("Make sure MySQL is running and the table exists")
    
    # Stop Spark session
    spark.stop()
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)