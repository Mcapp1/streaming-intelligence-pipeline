import os
print("📡 DEBUG: KAFKA_BOOTSTRAP_SERVERS =", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import pandas as pd
from snowflake_loader import SnowflakeLoader

class StreamingDataConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'streaming-trends',
            'reddit-discussions', 
            'piracy-sentiment',
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')],  # Add this
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='streaming-intelligence-consumer',
            auto_offset_reset='earliest'
        )
        self.snowflake_loader = SnowflakeLoader()
        
        # Batch processing
        self.batch_size = 10
        self.trends_batch = []
        self.discussions_batch = []
        self.piracy_batch = []
    
    def process_trends_message(self, message):
        """Process Google Trends message"""
        self.trends_batch.append(message)
        
        if len(self.trends_batch) >= self.batch_size:
            self.flush_trends_batch()
    
    def process_discussion_message(self, message):
        """Process Reddit discussion message"""
        self.discussions_batch.append(message)
        
        if len(self.discussions_batch) >= self.batch_size:
            self.flush_discussions_batch()
    
    def process_piracy_message(self, message):
        """Process piracy sentiment message"""
        self.piracy_batch.append(message)
        
        if len(self.piracy_batch) >= self.batch_size:
            self.flush_piracy_batch()
    
    def flush_trends_batch(self):
        if not self.trends_batch:
            return

        insert_sql = """
            INSERT INTO PLATFORM_TRENDS (
                source, platform, topic, sentiment, mention_count, 
                upvote_ratio_avg, score_avg, comment_count_avg, 
                title_sample, collection_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor = self.snowflake_loader.conn.cursor()

            for record in self.trends_batch:
                # Convert collection_timestamp safely
                try:
                    ts = float(record.get("collection_timestamp", time.time()))
                    collection_ts = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    collection_ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                cursor.execute(insert_sql, (
                    record.get("source"),
                    record.get("platform"),
                    record.get("topic"),
                    record.get("sentiment"),
                    record.get("mention_count"),
                    record.get("upvote_ratio_avg"),
                    record.get("score_avg"),
                    record.get("comment_count_avg"),
                    record.get("title_sample"),
                    collection_ts
                ))

            self.snowflake_loader.conn.commit()
            print(f"Flushed {len(self.trends_batch)} trends records to Snowflake")
            self.trends_batch = []

        except Exception as e:
            print(f"Error inserting trends batch: {e}")

    
    def flush_discussions_batch(self):
        """Insert discussions batch into Snowflake"""
        if not self.discussions_batch:
            return
            
        cursor = self.snowflake_loader.conn.cursor()
        
        for record in self.discussions_batch:
            insert_sql = """
            INSERT INTO REDDIT_STREAMING_DISCUSSIONS 
            (subreddit, title, score, num_comments, created_utc, url, selftext, collection_timestamp, data_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                record.get('subreddit'),
                record.get('title'),
                record.get('score'),
                record.get('num_comments'),
                record.get('created_utc'),
                record.get('url'),
                record.get('selftext'),
                record.get('collection_timestamp'),
                record.get('data_type')
            ))
        
        self.snowflake_loader.conn.commit()
        print(f"Flushed {len(self.discussions_batch)} discussion records to Snowflake")
        self.discussions_batch = []
    
    def flush_piracy_batch(self):
        """Insert piracy sentiment batch into Snowflake"""
        if not self.piracy_batch:
            return
            
        cursor = self.snowflake_loader.conn.cursor()
        
        for record in self.piracy_batch:
            insert_sql = """
            INSERT INTO REDDIT_PIRACY_SENTIMENT 
            (subreddit, search_term, title, score, num_comments, created_utc, selftext, collection_timestamp, data_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                record.get('subreddit'),
                record.get('search_term'),
                record.get('title'),
                record.get('score'),
                record.get('num_comments'),
                record.get('created_utc'),
                record.get('selftext'),
                record.get('collection_timestamp'),
                record.get('data_type')
            ))
        
        self.snowflake_loader.conn.commit()
        print(f"Flushed {len(self.piracy_batch)} piracy sentiment records to Snowflake")
        self.piracy_batch = []
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        print("Starting Kafka consumer...")
        
        try:
            for message in self.consumer:
                topic = message.topic
                value = message.value
                
                print(f"Received message from {topic}")
                
                if topic == 'streaming-trends':
                    self.process_trends_message(value)
                elif topic == 'reddit-discussions':
                    self.process_discussion_message(value)
                elif topic == 'piracy-sentiment':
                    self.process_piracy_message(value)
                    
        except KeyboardInterrupt:
            print("Stopping consumer...")
            # Flush any remaining batches
            self.flush_trends_batch()
            self.flush_discussions_batch()
            self.flush_piracy_batch()

# Test it
if __name__ == "__main__":
    consumer = StreamingDataConsumer()
    consumer.start_consuming()