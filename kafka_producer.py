import os
print("📡 DEBUG: KAFKA_BOOTSTRAP_SERVERS =", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
from kafka import KafkaProducer
import json
import time

class StreamingDataProducer:
    def __init__(self):
        # Only create Kafka producer in init
        self.producer = KafkaProducer(
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )
        print("✅ Kafka producer initialized")
        
    def initialize_collectors(self):
        # Create collectors separately, after Kafka is stable
        from pipeline_test import GoogleTrendsCollector
        from reddit_collector import RedditCollector
        
        self.trends_collector = GoogleTrendsCollector()
        self.reddit_collector = RedditCollector()
        print("✅ Data collectors initialized")
    
    def send_trends_data(self):
        """Send Google Trends data to Kafka"""
        print("Collecting and streaming Google Trends data...")
        
        trends_data = self.trends_collector.collect_all_trends()
        
        for df in trends_data:
            if not df.empty:
                # Convert DataFrame to records and send each row
                records = df.reset_index().to_dict('records')
                
                for record in records:
                    self.producer.send('streaming-trends', record)
                    print(f"Sent trends record: {record['data_type']}")
                
        self.producer.flush()
    
    def send_reddit_data(self):
        """Send Reddit data to Kafka"""
        print("Collecting and streaming Reddit data...")
        
        # Stream discussions
        discussions = self.reddit_collector.collect_streaming_discussions(limit=20)
        if not discussions.empty:
            records = discussions.to_dict('records')
            for record in records:
                self.producer.send('reddit-discussions', record)
                print(f"Sent discussion: {record['title'][:50]}...")
        
        # Stream piracy sentiment
        piracy = self.reddit_collector.collect_piracy_sentiment(limit=15)
        if not piracy.empty:
            records = piracy.to_dict('records')
            for record in records:
                self.producer.send('piracy-sentiment', record)
                print(f"Sent piracy sentiment: {record['title'][:50]}...")
        
        self.producer.flush()
    
    def run_pipeline(self):
        print("Starting streaming pipeline...")
        self.initialize_collectors()
        
        # Send data once, then exit (for testing)
        self.send_trends_data()
        self.send_reddit_data()
        print("✅ Pipeline completed successfully")

if __name__ == "__main__":
    producer = StreamingDataProducer()
    producer.run_pipeline()