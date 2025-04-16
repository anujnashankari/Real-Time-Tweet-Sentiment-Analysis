import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="sparkuser",
    password="Spark@123",
    database="tweetdb"
)
cursor = conn.cursor()

cursor.execute("SELECT sentiment, COUNT(*), AVG(polarity) FROM tweet_sentiments GROUP BY sentiment;")
results = cursor.fetchall()

print("\n=== FINAL COMPARISON REPORT ===")
print("Sentiment | Count | Avg Polarity")
print("----------------------------------")
for r in results:
    print(f"{r[0]:<9} | {r[1]:<5} | {r[2]:.4f}")

cursor.close()
conn.close()

