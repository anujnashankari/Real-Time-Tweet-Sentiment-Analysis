import time
import mysql.connector
import sys
import os

# Database connection properties
DB_HOST = "localhost"
DB_USER = "sparkuser"
DB_PASS = "Spark@123"
DB_NAME = "tweetdb"
DB_TABLE = "tweet_sentiments"

try:
    start_time = time.time()

    # Simulate streaming already running (via twitter_producer.py + spark streaming)
    print("Waiting 30 seconds for streaming mode to populate MySQL...")
    wait_time = 30
    
    # Show a progress bar
    for i in range(wait_time):
        progress = int((i / wait_time) * 20)
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('='*progress, 5*progress))
        sys.stdout.flush()
        time.sleep(1)
    
    print("\nWait completed. Retrieving results...")

    # Connect to MySQL and query results
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Count total tweets
        cursor.execute(f"SELECT COUNT(*) FROM {DB_TABLE}")
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            print("No data found in database. Make sure the streaming process is running.")
            sys.exit(1)
            
        print(f"\nTotal tweets analyzed: {total_count}")

        # Get sentiment distribution
        cursor.execute(f"""
            SELECT sentiment, COUNT(*) as tweet_count, AVG(polarity) as average_polarity
            FROM {DB_TABLE}
            GROUP BY sentiment
            ORDER BY tweet_count DESC;
        """)
        streaming_results = cursor.fetchall()

        print("\n=== STREAMING MODE RESULTS ===")
        print("Sentiment | Count | Avg Polarity")
        print("-" * 40)
        
        for row in streaming_results:
            print(f"{row[0]:10} | {row[1]:5} | {row[2]:.4f}")
            
        # Calculate percentages
        print("\nSentiment Distribution (%):")
        for row in streaming_results:
            percentage = (row[1] / total_count) * 100
            print(f"{row[0]:10}: {percentage:.2f}%")

        end_time = time.time()
        print(f"\nStreaming Mode Evaluation Time: {end_time - start_time:.2f} seconds")

        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        print("Make sure MySQL is running and the table exists")
        
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)