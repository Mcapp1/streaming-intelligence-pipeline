from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import boto3
import json
import os

class GoogleTrendsCollector:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360)
        
    def get_platform_trends(self, timeframe='today 3-m'):
        """Get search trends for streaming platforms"""
        platforms = ['Netflix', 'Disney Plus', 'Hulu', 'HBO Max', 'Amazon Prime Video']
        
        self.pytrends.build_payload(platforms, cat=0, timeframe=timeframe, geo='US')
        data = self.pytrends.interest_over_time()
        
        if not data.empty:
            data = data.drop('isPartial', axis=1)
            data['data_type'] = 'platform_interest'
            data['collection_timestamp'] = datetime.now()
        
        return data
    
    def get_free_streaming_trends(self, timeframe='today 3-m'):
        """Get search trends for free streaming terms"""
        free_terms = ['free netflix', 'watch movies free', 'free streaming sites', 'free movies online']
        
        self.pytrends.build_payload(free_terms, cat=0, timeframe=timeframe, geo='US')
        data = self.pytrends.interest_over_time()
        
        if not data.empty:
            data = data.drop('isPartial', axis=1)
            data['data_type'] = 'free_streaming_interest'
            data['collection_timestamp'] = datetime.now()
            
        return data
    
    def collect_all_trends(self):
        """Collect all trends data with rate limiting"""
        all_data = []
        
        # Get platform trends
        platform_data = self.get_platform_trends()
        if not platform_data.empty:
            all_data.append(platform_data)
        
        # Rate limiting - Google Trends has limits
        time.sleep(2)
        
        # Get free streaming trends
        free_data = self.get_free_streaming_trends()
        if not free_data.empty:
            all_data.append(free_data)
            
        return all_data

class S3DataLoader:
    def __init__(self, bucket_name='streaming-intelligence-raw-data'):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        
    def save_trends_data(self, trends_data_list):
        """Save Google Trends data to S3"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for data in trends_data_list:
            if not data.empty:
                data_type = data['data_type'].iloc[0]
                
                # Save as both CSV and JSON
                csv_key = f"google_trends/{data_type}/{timestamp}.csv"
                json_key = f"google_trends/{data_type}/{timestamp}.json"
                
                # CSV format
                csv_buffer = data.to_csv(index=True)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=csv_key,
                    Body=csv_buffer
                )
                
                # JSON format
                json_data = data.reset_index().to_json(orient='records', date_format='iso')
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=json_key,
                    Body=json_data
                )
                
                print(f"Saved {data_type} data to s3://{self.bucket_name}/{csv_key}")

# Test the full pipeline
if __name__ == "__main__":
    # Collect data
    collector = GoogleTrendsCollector()
    trends_data = collector.collect_all_trends()
    
    # Save to S3
    loader = S3DataLoader()
    loader.save_trends_data(trends_data)
    
    print("Pipeline completed!")