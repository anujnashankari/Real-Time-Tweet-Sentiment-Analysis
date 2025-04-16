from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker
import random
import time
import json
import os
import sys
import argparse
import tweepy

# Kafka configuration
KAFKA_SERVER = 'localhost:9092'
TOPICS = ['raw_tweets', 'user_data']

# Function to create Kafka topics if they don't exist
def create_topics():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        
        # Create topics that don't exist
        new_topics = []
        for topic in TOPICS:
            if topic not in existing_topics:
                new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
            print(f"Created topics: {', '.join([t.name for t in new_topics])}")
        else:
            print("All required topics already exist")
            
        admin_client.close()
    except Exception as e:
        print(f"Error creating topics: {e}")
        # Continue with execution even if topic creation fails

# Main function
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Twitter Producer")
    parser.add_argument("--producer-type", type=str, default="tweet",
                        choices=["tweet", "user"],
                        help="Type of producer: 'tweet' for tweet data, 'user' for user data")
    parser.add_argument("--instance-id", type=int, default=1,
                        help="Instance ID of this producer")
                        
    args = parser.parse_args()
    producer_type = args.producer_type
    instance_id = args.instance_id
    
    try:
        # Create topics first
        print(f"Setting up Kafka topics for {producer_type} producer (ID: {instance_id})...")
        create_topics()
        
        # Initialize producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Faker for generating synthetic data
        fake = Faker()
        keywords = ['Python', 'AI', 'Elections', 'ChatGPT', 'Climate', 'Sports', 'Tech']
        
        # Hashtags to use occasionally
        hashtags = ['#DataScience', '#BigData', '#AI', '#MachineLearning', '#Python', 
                    '#Kafka', '#Spark', '#CloudComputing', '#Analytics']
        
        print(f"Sending fake {producer_type} data to Kafka topics...")
        counter = 0
        
        while True:
            # Generate user data
            username = fake.user_name()
            location = fake.city() + ", " + fake.country_code()
            followers = random.randint(5, 15000)
            verified = random.random() > 0.9  # 10% chance of being verified
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            
            if producer_type == "tweet":
                # Choose a keyword and create hashtag
                topic = random.choice(keywords)
                main_hashtag = f"#{topic}"
                
                # Occasionally add additional hashtags
                extra_hashtags = ""
                if random.random() > 0.7:  # 30% chance to add extra hashtags
                    num_extra = random.randint(1, 3)
                    selected_hashtags = random.sample(hashtags, min(num_extra, len(hashtags)))
                    extra_hashtags = " " + " ".join(selected_hashtags)
                
                # Create tweet text with hashtags
                tweet_text = f"{fake.sentence()} {main_hashtag}{extra_hashtags}"
                
                # Create a rich tweet object
                tweet = {
                    'id': counter,
                    'text': tweet_text,
                    'timestamp': timestamp,
                    'user': username,
                    'location': location,
                    'followers': followers,
                    'verified': verified,
                    'retweets': random.randint(0, 100),
                    'favorites': random.randint(0, 500)
                }
                
                # Send tweet to raw_tweets topic
                print(f"> Tweet Producer #{instance_id} sending tweet #{counter}: {tweet_text[:50]}...")
                producer.send('raw_tweets', value=tweet)
                
            elif producer_type == "user":
                # Create rich user data object
                user_data = {
                    'username': username,
                    'location': location,
                    'followers': followers,
                    'verified': verified,
                    'timestamp': timestamp,
                    'joined_date': fake.date_time_this_decade().strftime("%Y-%m-%d"),
                    'profile_views': random.randint(10, 50000),
                    'bio': fake.text(max_nb_chars=160),
                    'total_tweets': random.randint(1, 10000)
                }
                
                # Send user data to user_data topic
                print(f"> User Data Producer #{instance_id} sending data for user #{counter}: {username}...")
                producer.send('user_data', value=user_data)
            
            counter += 1
            time.sleep(1)  # Send data every second
                
    except KeyboardInterrupt:
        print(f"\n{producer_type.capitalize()} producer interrupted. Shutting down...")
        if 'producer' in locals():
            producer.close()
        sys.exit(0)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure Kafka is running on localhost:9092")
        sys.exit(1)

if __name__ == "__main__":
    main()