import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import snowflake.connector
import os
from datetime import datetime
import streamlit as st

# Load environment variables for Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER', 'MCAPPARELLI1'),
        password=os.getenv('SNOWFLAKE_PASSWORD', 'Limabellabean55'),
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'XKQBOYS-LX35596'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'STREAMING_INTELLIGENCE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA')
    )

@st.cache_data
def load_data():
    """Load data from Snowflake"""
    conn = get_snowflake_connection()
    
    # Platform trends data - using correct UPPERCASE column names
    platform_trends = pd.read_sql("""
        SELECT DATE, NETFLIX, DISNEY_PLUS, HULU, HBO_MAX, AMAZON_PRIME_VIDEO, 
               COLLECTION_TIMESTAMP, LOADED_AT
        FROM PLATFORM_TRENDS 
        ORDER BY DATE
    """, conn)
    
    # Reddit discussions - using correct UPPERCASE column names
    discussions = pd.read_sql("""
        SELECT SUBREDDIT, TITLE, SCORE, NUM_COMMENTS, CREATED_UTC, 
               COLLECTION_TIMESTAMP, LOADED_AT
        FROM REDDIT_STREAMING_DISCUSSIONS 
        ORDER BY LOADED_AT DESC
        LIMIT 100
    """, conn)
    
    # Piracy sentiment - using correct UPPERCASE column names
    piracy = pd.read_sql("""
        SELECT SUBREDDIT, SEARCH_TERM, TITLE, SCORE, NUM_COMMENTS, 
               CREATED_UTC, COLLECTION_TIMESTAMP, LOADED_AT
        FROM REDDIT_PIRACY_SENTIMENT 
        ORDER BY LOADED_AT DESC
        LIMIT 100
    """, conn)
    
    conn.close()
    return platform_trends, discussions, piracy

def create_platform_competition_chart(df):
    """Create platform competition visualization"""
    fig = go.Figure()
    
    # Add traces for each platform - using UPPERCASE column names
    platforms = ['NETFLIX', 'DISNEY_PLUS', 'HULU', 'HBO_MAX', 'AMAZON_PRIME_VIDEO']
    colors = ['#E50914', '#113CCF', '#1CE783', '#9D2D90', '#00A8E1']
    platform_names = ['Netflix', 'Disney+', 'Hulu', 'HBO Max', 'Prime Video']
    
    for i, platform in enumerate(platforms):
        fig.add_trace(go.Scatter(
            x=df['DATE'],
            y=df[platform],
            mode='lines+markers',
            name=platform_names[i],
            line=dict(color=colors[i], width=3),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title={
            'text': '🏆 Streaming Platform Search Interest Over Time',
            'x': 0.5,
            'font': {'size': 24, 'color': '#1f2937'}
        },
        xaxis_title='Date',
        yaxis_title='Search Interest (0-100)',
        height=500,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template='plotly_white'
    )
    
    return fig

def create_platform_market_share(df):
    """Create platform market share pie chart"""
    # Calculate average interest for each platform - using UPPERCASE column names
    platform_avg = {
        'Netflix': df['NETFLIX'].mean(),
        'Disney+': df['DISNEY_PLUS'].mean(),
        'Hulu': df['HULU'].mean(),
        'HBO Max': df['HBO_MAX'].mean(),
        'Prime Video': df['AMAZON_PRIME_VIDEO'].mean()
    }
    
    colors = ['#E50914', '#113CCF', '#1CE783', '#9D2D90', '#00A8E1']
    
    fig = go.Figure(data=[go.Pie(
        labels=list(platform_avg.keys()),
        values=list(platform_avg.values()),
        marker_colors=colors,
        textinfo='label+percent',
        textfont_size=14,
        hole=0.3
    )])
    
    fig.update_layout(
        title={
            'text': '📊 Average Search Interest Market Share',
            'x': 0.5,
            'font': {'size': 20, 'color': '#1f2937'}
        },
        height=400,
        showlegend=False
    )
    
    return fig

def create_reddit_sentiment_analysis(discussions, piracy):
    """Create Reddit sentiment analysis"""
    # Top subreddits by engagement - using UPPERCASE column names
    subreddit_engagement = discussions.groupby('SUBREDDIT').agg({
        'SCORE': 'mean',
        'NUM_COMMENTS': 'mean',
        'TITLE': 'count'
    }).round(2)
    subreddit_engagement.columns = ['Avg Score', 'Avg Comments', 'Post Count']
    subreddit_engagement = subreddit_engagement.sort_values('Post Count', ascending=False)
    
    # Piracy sentiment keywords - using UPPERCASE column names
    piracy_keywords = piracy.groupby('SEARCH_TERM').size().sort_values(ascending=False)
    
    return subreddit_engagement, piracy_keywords

def create_piracy_sentiment_chart(piracy_keywords):
    """Create piracy sentiment visualization"""
    fig = go.Figure([go.Bar(
        x=piracy_keywords.values,
        y=piracy_keywords.index,
        orientation='h',
        marker_color='#dc2626',
        text=piracy_keywords.values,
        textposition='inside'
    )])
    
    fig.update_layout(
        title={
            'text': '🏴‍☠️ Alternative Viewing Search Patterns',
            'x': 0.5,
            'font': {'size': 20, 'color': '#1f2937'}
        },
        xaxis_title='Number of Mentions',
        yaxis_title='Search Terms',
        height=400,
        template='plotly_white'
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="Streaming Intelligence Dashboard",
        page_icon="📺",
        layout="wide"
    )
    
    st.title("📺 Streaming Platform Intelligence Dashboard")
    st.markdown("### Real-time analysis of streaming platform competition and consumer behavior")
    
    # Load data
    with st.spinner("Loading real-time data from Snowflake..."):
        platform_trends, discussions, piracy = load_data()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Platform Trends Records", 
            f"{len(platform_trends):,}",
            delta="Real-time data"
        )
    
    with col2:
        st.metric(
            "Reddit Discussions", 
            f"{len(discussions):,}",
            delta="Last 100 posts"
        )
    
    with col3:
        st.metric(
            "Piracy Sentiment Posts", 
            f"{len(piracy):,}",
            delta="Alternative viewing"
        )
    
    with col4:
        netflix_dominance = platform_trends['NETFLIX'].mean() / platform_trends['DISNEY_PLUS'].mean()
        st.metric(
            "Netflix vs Disney+ Ratio", 
            f"{netflix_dominance:.1f}x",
            delta="Search interest"
        )
    
    # Platform Competition Analysis
    st.header("🏆 Platform Competition Analysis")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig_competition = create_platform_competition_chart(platform_trends)
        st.plotly_chart(fig_competition, use_container_width=True)
    
    with col2:
        fig_market_share = create_platform_market_share(platform_trends)
        st.plotly_chart(fig_market_share, use_container_width=True)
    
    # Key insights
    st.info(f"""
    **Key Insights:**
    - Netflix dominates with {platform_trends['NETFLIX'].mean():.1f} average search interest
    - Disney+ averages {platform_trends['DISNEY_PLUS'].mean():.1f}, showing {netflix_dominance:.1f}x lower interest
    - Hulu maintains steady {platform_trends['HULU'].mean():.1f} interest level
    """)
    
    # Reddit Analysis
    st.header("💬 Social Media Intelligence")
    
    subreddit_engagement, piracy_keywords = create_reddit_sentiment_analysis(discussions, piracy)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Discussion Communities")
        st.dataframe(subreddit_engagement, use_container_width=True)
    
    with col2:
        fig_piracy = create_piracy_sentiment_chart(piracy_keywords)
        st.plotly_chart(fig_piracy, use_container_width=True)
    
    # Recent Discussions Sample - using UPPERCASE column names
    st.header("📱 Recent Platform Discussions")
    
    # Show recent high-engagement discussions
    top_discussions = discussions.nlargest(5, 'SCORE')[['SUBREDDIT', 'TITLE', 'SCORE', 'NUM_COMMENTS']]
    
    for idx, row in top_discussions.iterrows():
        with st.expander(f"r/{row['SUBREDDIT']} - {row['TITLE'][:100]}..."):
            st.write(f"**Score:** {row['SCORE']} | **Comments:** {row['NUM_COMMENTS']}")
            st.write(f"**Platform:** {row['SUBREDDIT']}")
    
    # Data freshness
    st.sidebar.header("📊 Data Pipeline Status")
    latest_update = platform_trends['LOADED_AT'].max()
    st.sidebar.success(f"Last Updated: {latest_update}")
    st.sidebar.info(f"Total Records Processed: {len(platform_trends) + len(discussions) + len(piracy):,}")
    
    # Technical details
    with st.sidebar.expander("🔧 Technical Stack"):
        st.write("""
        **Data Sources:**
        - Google Trends API
        - Reddit API
        
        **Processing:**
        - Apache Kafka
        - Kubernetes
        
        **Storage:**
        - Snowflake Data Warehouse
        
        **Visualization:**
        - Streamlit + Plotly
        """)

if __name__ == "__main__":
    main()