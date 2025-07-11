import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import time


class RedditCollector:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
    def collect_streaming_discussions(self, limit=100):
        """Collect recent discussions about streaming platforms"""
        
        # Subreddits to monitor
        subreddits = ['netflix', 'DisneyPlus', 'hulu', 'HBOMax', 'cordcutters', 'television']
        
        all_posts = []
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get recent hot posts
                for post in subreddit.hot(limit=limit//len(subreddits)):
                    post_data = {
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'created_utc': datetime.fromtimestamp(post.created_utc),
                        'url': post.url,
                        'selftext': post.selftext[:500] if post.selftext else '',  # First 500 chars
                        'collection_timestamp': datetime.now(),
                        'data_type': 'reddit_streaming_discussion'
                    }
                    all_posts.append(post_data)
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"Error collecting from r/{subreddit_name}: {e}")
                continue
        
        return pd.DataFrame(all_posts)
    
    def collect_piracy_sentiment(self, limit=50):
        """Collect discussions about free streaming and piracy alternatives"""
        
        # Search terms that indicate "alternative viewing" interest
        search_terms = [
            'free streaming sites',
            'watch free online',
            'streaming too expensive',
            'cancelled subscription',
            'cord cutting'
        ]
        
        all_posts = []
        
        # Search across multiple subreddits
        subreddits = ['cordcutters', 'television', 'movies', 'Piracy']
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                for term in search_terms:
                    for post in subreddit.search(term, sort='new', time_filter='week', limit=limit//len(search_terms)):
                        post_data = {
                            'subreddit': subreddit_name,
                            'search_term': term,
                            'title': post.title,
                            'score': post.score,
                            'num_comments': post.num_comments,
                            'created_utc': datetime.fromtimestamp(post.created_utc),
                            'selftext': post.selftext[:500] if post.selftext else '',
                            'collection_timestamp': datetime.now(),
                            'data_type': 'reddit_piracy_sentiment'
                        }
                        all_posts.append(post_data)
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"Error searching in r/{subreddit_name}: {e}")
                continue
        
        return pd.DataFrame(all_posts)

# Test it
if __name__ == "__main__":
    collector = RedditCollector()
    
    print("Collecting streaming discussions...")
    streaming_data = collector.collect_streaming_discussions(limit=50)
    print(f"Collected {len(streaming_data)} streaming posts")
    print(streaming_data.head())
    
    print("\nCollecting piracy sentiment...")
    piracy_data = collector.collect_piracy_sentiment(limit=30)
    print(f"Collected {len(piracy_data)} piracy-related posts")
    print(piracy_data.head())