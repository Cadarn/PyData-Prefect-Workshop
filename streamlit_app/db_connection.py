import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

def get_mongo_client():
    # Get the MongoDB URI from environment variables
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    return client

def fetch_tweets_data():
    client = get_mongo_client()
    db = client['Prefect-tutorial']  
    collection = db['sentiment_airline_tweets']

    # Fetch the data
    tweets_data = collection.find()
    
    # Convert the MongoDB cursor to a Pandas DataFrame
    df = pd.DataFrame(list(tweets_data))

    # Ensure proper datetime conversion for the 'tweet_created' field
    df['tweet_timestamp'] = pd.to_datetime(df['tweet_timestamp'])

    return df
