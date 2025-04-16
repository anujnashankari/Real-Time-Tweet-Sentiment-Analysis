Twitter Sentiment Analysis using Apache Kafka, Spark Streaming, and MySQL
This project focuses on real-time and batch sentiment analysis of tweets using Apache Spark Streaming, Apache Kafka, and MySQL. The system performs sentiment classification (Positive, Negative, Neutral) on live tweet data and stores results for further analysis and comparison.

Features
Real-time tweet ingestion via Kafka.
Sentiment classification using TextBlob.
Storage of analyzed tweets in MySQL.
Comparison between Streaming Mode and Batch Mode.
Polarity-based sentiment distribution and evaluation.

Results and Discussion
Streaming Mode analyzed data in ~30 seconds.
Batch Mode analyzed the same dataset in ~7 seconds.
Both modes produced consistent sentiment distributions.

The project successfully demonstrates sentiment classification using TextBlob on real-time and batch tweet data. Both modes have trade-offs: streaming for real-time insights and batch for fast offline processing.

How to Run:
Make the Python script executable "chmod +x run_pipeline.py"
Execute the pipeline "python run_pipeline.py"
Make sure Kafka, MySQL, and Spark services are running before starting the script.
