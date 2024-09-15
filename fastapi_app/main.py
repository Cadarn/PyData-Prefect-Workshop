from pathlib import Path
import random
import time
from typing import Optional, Union

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def load_data(datapath: Path)->pd.DataFrame:
    df = pd.read_csv(datapath)
    df.loc[:, "tweet_timestamp"] = pd.to_datetime(df["tweet_created"])
    columns = ["tweet_id", "airline_sentiment", "airline_sentiment_confidence",
               "airline", "name", "text", "retweet_count", 
               "tweet_timestamp", "tweet_coord"
               ]
    return df[columns].fillna(-1)

DATAPATH = Path("/app/data")

app = FastAPI()

# Rate limiting middleware
last_request_time = datetime.min

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    global last_request_time
    now = datetime.now()
    if now - last_request_time < timedelta(seconds=0.1):
        return JSONResponse(status_code=429, content={"message": "Rate limit exceeded"})
    last_request_time = now
    response = await call_next(request)
    return response

# CORS settings
origins = ["*"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["*"], allow_headers=["*"])

# GZIP middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Load the CSV file
df = load_data(DATAPATH / 'airline_tweets.csv')
num_records = len(df)
current_index = 0
loop_count = 1

class Tweet(BaseModel):
    tweet_id: int
    airline_sentiment: str
    airline_sentiment_confidence: float
    airline: str
    name: str
    text: str
    retweet_count: Optional[int]
    tweet_timestamp: datetime
    tweet_coord: Optional[Union[tuple[float], float]]
    loop: int

@app.on_event("startup")
async def startup_event():
    global df, num_records, current_index, loop_count
    df = load_data(DATAPATH / 'airline_tweets.csv')
    num_records = len(df)
    current_index = 0
    loop_count = 1

@app.get("/get_tweet", response_model=Tweet)
async def get_tweet():
    global current_index, loop_count

    # Simulate random delay
    delay = random.uniform(0.01, 2)
    time.sleep(delay)

    # Simulate 25% chance of returning 404 error
    if random.random() < 0.25:
        raise HTTPException(status_code=404, detail="Tweet not found")

    tweet = df.iloc[current_index]
    current_index += 1

    if current_index >= num_records:
        current_index = 0
        loop_count += 1

    return Tweet(**tweet.to_dict(), loop=loop_count)
