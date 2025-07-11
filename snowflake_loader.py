import snowflake.connector
import pandas as pd
import boto3
from io import StringIO
import os
from datetime import datetime

# Load environment variables

class SnowflakeLoader:
    def __init__(self):
        # Snowflake connection using environment variables
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        self.s3_client = boto3.client('s3')
        
    def load_platform_trends_from_s3(self, s3_key, bucket='streaming-intelligence-raw-data'):
        """Load platform trends data from S3 CSV into Snowflake"""
        
        # Download CSV from S3
        response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Read into DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Prepare data for Snowflake
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            insert_sql = """
            INSERT INTO PLATFORM_TRENDS 
            (date, netflix, disney_plus, hulu, hbo_max, amazon_prime_video, data_type, collection_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                row['date'],
                row['Netflix'],
                row['Disney Plus'], 
                row['Hulu'],
                row['HBO Max'],
                row['Amazon Prime Video'],
                row['data_type'],
                row['collection_timestamp']
            ))
        
        self.conn.commit()
        print(f"Loaded {len(df)} platform trend records to Snowflake")
        
    def load_free_streaming_trends_from_s3(self, s3_key, bucket='streaming-intelligence-raw-data'):
        """Load free streaming trends data from S3 CSV into Snowflake"""
        
        # Download CSV from S3
        response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Read into DataFrame  
        df = pd.read_csv(StringIO(csv_content))
        
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            insert_sql = """
            INSERT INTO FREE_STREAMING_TRENDS 
            (date, free_netflix, watch_movies_free, free_streaming_sites, free_movies_online, data_type, collection_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                row['date'],
                row['free netflix'],
                row['watch movies free'],
                row['free streaming sites'],
                row['free movies online'],
                row['data_type'],
                row['collection_timestamp']
            ))
        
        self.conn.commit()
        print(f"Loaded {len(df)} free streaming trend records to Snowflake")

# Test it
if __name__ == "__main__":
    loader = SnowflakeLoader()
    
    # Load the files you created
    loader.load_platform_trends_from_s3('google_trends/platform_interest/20250710_152125.csv')
    loader.load_free_streaming_trends_from_s3('google_trends/free_streaming_interest/20250710_152125.csv')
    
    print("Data loaded to Snowflake!")