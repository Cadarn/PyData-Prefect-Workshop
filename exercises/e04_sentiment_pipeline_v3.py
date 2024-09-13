import sys
sys.path.append("..")

import json
from prefect import task, flow, get_run_logger
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from config.kafka_config import KAFKA_SERVERS
from prefect.blocks.system import Secret
from config.mongodb_config import MONGO_URI, DB_NAME
from e02b_sentiment_pipeline_v2 import sentiment_analysis

KAFKA_TOPIC_AIRLINES = "raw_airline_tweet"

# DO NOT EDIT
def get_mongo_db(uri: str) -> MongoClient:
    """
    Returns a MongoDB client connected to the specified URI.
    
    Args:
        uri (str): The MongoDB connection URI.
        
    Returns:
        MongoClient: The MongoDB client instance.
    """
    client = MongoClient(uri, server_api=ServerApi('1'))
    return client

# DO NOT EDIT
def get_kafka_consumer(kafka_topic: str) -> KafkaConsumer:
    """
    Returns a Kafka consumer configured for the specified topic.
    
    Args:
        kafka_topic (str): The Kafka topic to consume messages from.
        
    Returns:
        KafkaConsumer: The Kafka consumer instance.
    """
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="PREFECT-DEV",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

# DO NOT EDIT
@task(name="Write Message to MongoDB", description="Writes a record to MongoDB.")
def write_msg_to_mongo(record: dict, client: MongoClient) -> None:
    """
    Writes a record to MongoDB.
    
    Args:
        record (Dict): The record to be inserted into MongoDB.
        client (MongoClient): The MongoDB client instance.
        
    Returns:
        None
    """
    db = client["Prefect-tutorial"]
    collection = db["sentiment_airline_tweets"]
    collection.insert_one(record)


@task(name="Label Sentiment") # Add a description
def label_sentiment(score: float) -> str:
    """
    Labels sentiment based on the sentiment score.
    
    Args:
        score (float): The sentiment score.
        
    Returns:
        str: The sentiment label.
    """
    if score > # Set your thresholds:
        label = "positive"
    elif score < #Set your  thresholds:
        label = "negative"
    else:
        label = "neutral"
    print(f"Converting sentiment score = {score} to {label}")
    return label
    


@task #Add any parameters you need 
def consume_airline_tweets(kafka_topic: str = KAFKA_TOPIC_AIRLINES):
    """
    Consumes airline tweets from Kafka, processes them, and stores them in MongoDB.
    
    Args:
        kafka_topic (str): The Kafka topic to consume messages from.
    """
    _MONGO_URI = "" # Load the Secret from the password Block
    client = get_mongo_db(_MONGO_URI)
    consumer = get_kafka_consumer(kafka_topic)
    print(f"Starting to consume messages from Kafka topic: {kafka_topic}")

    while True:
        poll_result = consumer.poll(timeout_ms=5000)
        for _, messages in poll_result.items():
            for msg in messages:
                sentiment_score = 0 # You need to process the tweet content field that is in the message, take a look at a message
                # example by using your browser at https://localhost:8000/get_tweet
                # tip: you need to process msg.value to get to the python dictionary
                sentiment_label = label_sentiment(sentiment_score)
                new_msg = {}# Take all of the raw message fields and add the additional sentiment data before pushing this to Mongo
                            # new_msg needs to be a dictionary
                write_msg_to_mongo(new_msg, client=client)
                print(f"Processed message: {msg.value}")

@flow(name="Monitor Airline Tweets", description="Monitors airline tweets and processes them every 30 seconds.", log_prints=True)
def monitor_airline_tweets():
    """
    Monitors airline tweets and processes them every 30 seconds.
    """
    consume_airline_tweets(kafka_topic=KAFKA_TOPIC_AIRLINES)
    

if __name__ == "__main__":
    monitor_airline_tweets()
