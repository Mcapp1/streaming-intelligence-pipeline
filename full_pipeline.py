from pipeline_test import GoogleTrendsCollector, S3DataLoader
from reddit_collector import RedditCollector
import pandas as pd
from datetime import datetime

class FullStreamingPipeline:
    def __init__(self):
        self.trends_collector = GoogleTrendsCollector()
        self.reddit_collector = RedditCollector()
        self.s3_loader = S3DataLoader()
        
    def collect_all_data(self):
        """Collect data from all sources"""
        all_data = []
        
        print("Collecting Google Trends data...")
        trends_data = self.trends_collector.collect_all_trends()
        all_data.extend(trends_data)
        
        print("Collecting Reddit streaming discussions...")
        reddit_streaming = self.reddit_collector.collect_streaming_discussions(limit=50)
        if not reddit_streaming.empty:
            all_data.append(reddit_streaming)
            
        print("Collecting Reddit piracy sentiment...")
        reddit_piracy = self.reddit_collector.collect_piracy_sentiment(limit=30)
        if not reddit_piracy.empty:
            all_data.append(reddit_piracy)
            
        return all_data
    
    def save_reddit_data_to_s3(self, reddit_data):
        """Save Reddit data to S3 with proper structure"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for data in reddit_data:
            if isinstance(data, pd.DataFrame) and not data.empty:
                data_type = data['data_type'].iloc[0]
                
                if 'reddit' in data_type:
                    # Save Reddit data
                    csv_key = f"reddit_data/{data_type}/{timestamp}.csv"
                    json_key = f"reddit_data/{data_type}/{timestamp}.json"
                    
                    # CSV format
                    csv_buffer = data.to_csv(index=False)
                    self.s3_loader.s3_client.put_object(
                        Bucket=self.s3_loader.bucket_name,
                        Key=csv_key,
                        Body=csv_buffer
                    )
                    
                    # JSON format
                    json_data = data.to_json(orient='records', date_format='iso')
                    self.s3_loader.s3_client.put_object(
                        Bucket=self.s3_loader.bucket_name,
                        Key=json_key,
                        Body=json_data
                    )
                    
                    print(f"Saved {data_type} data to s3://{self.s3_loader.bucket_name}/{csv_key}")
    
    def run_full_pipeline(self):
        """Run the complete data collection pipeline"""
        print("Starting full streaming intelligence pipeline...")
        
        # Collect all data
        all_data = self.collect_all_data()
        
        # Separate trends and Reddit data
        trends_data = [data for data in all_data if hasattr(data, 'columns') and 'Netflix' in data.columns]
        reddit_data = [data for data in all_data if hasattr(data, 'columns') and 'subreddit' in data.columns]
        
        # Save to S3
        if trends_data:
            self.s3_loader.save_trends_data(trends_data)
            
        if reddit_data:
            self.save_reddit_data_to_s3(reddit_data)
        
        print(f"Pipeline completed! Collected {len(trends_data)} trends datasets and {len(reddit_data)} Reddit datasets")

# Test it
if __name__ == "__main__":
    pipeline = FullStreamingPipeline()
    pipeline.run_full_pipeline()