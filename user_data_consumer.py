from kafka import KafkaConsumer, KafkaProducer
import json
import mysql.connector
import time
import sys

# Database connection properties
DB_HOST = "localhost"
DB_USER = "sparkuser"
DB_PASS = "Spark@123"
DB_NAME = "tweetdb"

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
INPUT_TOPIC = "user_data"
OUTPUT_TOPIC = "processed_user_data"

try:
    print("Starting User Data Consumer...")
    
    # Connect to MySQL
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    cursor = conn.cursor()
    
    # Create user_data table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255),
        location VARCHAR(255),
        tweet_count INT DEFAULT 1,
        last_tweet_timestamp VARCHAR(50),
        last_tweet TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Initialize Kafka consumer and producer
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='user_data_consumer_group'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print(f"Consuming tweets from '{INPUT_TOPIC}' and extracting user data...")
    
    # Process messages
    try:
        for message in consumer:
            tweet_data = message.value
            
            # Extract user data
            user = tweet_data.get('user', '')
            location = tweet_data.get('location', '')
            timestamp = tweet_data.get('timestamp', '')
            text = tweet_data.get('text', '')
            
            if user:
                # Check if user exists in database
                cursor.execute("SELECT id, tweet_count FROM user_data WHERE username = %s", (user,))
                user_record = cursor.fetchone()
                
                if user_record:
                    # Update existing user
                    user_id, tweet_count = user_record
                    new_count = tweet_count + 1
                    cursor.execute(
                        "UPDATE user_data SET tweet_count = %s, location = %s, last_tweet_timestamp = %s, last_tweet = %s WHERE id = %s",
                        (new_count, location, timestamp, text, user_id)
                    )
                else:
                    # Insert new user
                    cursor.execute(
                        "INSERT INTO user_data (username, location, last_tweet_timestamp, last_tweet) VALUES (%s, %s, %s, %s)",
                        (user, location, timestamp, text)
                    )
                
                conn.commit()
                
                # Publish user data to user_data topic
                user_data = {
                    'username': user,
                    'location': location,
                    'tweet_count': tweet_count if user_record else 1,
                    'last_activity': timestamp
                }
                producer.send(OUTPUT_TOPIC, value=user_data)
                
                print(f"Processed user data for: {user} from {location}")
            
    except KeyboardInterrupt:
        print("\nUser data consumer interrupted. Shutting down...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        producer.close()
        print("User data consumer stopped.")
        
except Exception as e:
    print(f"Error: {e}")
    print("Make sure Kafka and MySQL are running")
    sys.exit(1)