import sys
sys.path.append("..")

import json
from prefect import task, flow, get_run_logger
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from config.kafka_config import KAFKA_SERVERS, SASL_MECHANISM, SECURITY_PROTOCOL, SASL_PLAIN_PASSWORD, SASL_PLAIN_USERNAME
from config.mongodb_config import MONGO_URI, DB_NAME
from e02b_sentiment_pipeline_v2 import sentiment_analysis

KAFKA_TOPIC_AIRLINES = "raw_airline_tweet"

# DO NOT EDIT
def get_mongo_db(uri: str = MONGO_URI) -> MongoClient:
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
        sasl_mechanism=SASL_MECHANISM,
        security_protocol=SECURITY_PROTOCOL,
        sasl_plain_username=SASL_PLAIN_USERNAME,
        sasl_plain_password=SASL_PLAIN_PASSWORD,
        group_id="PREFECT-DEV",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

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


@task(name="Label Sentiment", description="Labels sentiment based on the sentiment score.")
def label_sentiment(score: float) -> str:
    """
    Labels sentiment based on the sentiment score.
    
    Args:
        score (float): The sentiment score.
        
    Returns:
        str: The sentiment label.
    """
    if score > 0.15:
        label = "positive"
    elif score < -0.15:
        label = "negative"
    else:
        label = "neutral"
    print(f"Converting sentiment score = {score} to {label}")
    return label
    


@task(name="Consume Airline Tweets", description="Consumes airline tweets from Kafka, processes them, and stores them in MongoDB.")
def consume_airline_tweets(kafka_topic: str = KAFKA_TOPIC_AIRLINES):
    """
    Consumes airline tweets from Kafka, processes them, and stores them in MongoDB.
    
    Args:
        kafka_topic (str): The Kafka topic to consume messages from.
    """
    consumer = get_kafka_consumer(kafka_topic)
    client = get_mongo_db()
    print(f"Starting to consume messages from Kafka topic: {kafka_topic}")

    while True:
        poll_result = consumer.poll(timeout_ms=5000)
        for context, messages in poll_result.items():
            for msg in messages:
                sentiment_score = sentiment_analysis(msg.value.get("text"))
                sentiment_label = label_sentiment(sentiment_score)
                msg.value.update({"sentiment_score": sentiment_score, "sentiment_label": sentiment_label})
                write_msg_to_mongo(msg.value, client=client)
                print(f"Processed message: {msg.value}")

@flow(name="Monitor Airline Tweets", description="Monitors airline tweets and processes them every 30 seconds.", log_prints=True)
def monitor_airline_tweets():
    """
    Monitors airline tweets and processes them every 30 seconds.
    """
    consume_airline_tweets(kafka_topic=KAFKA_TOPIC_AIRLINES)
    

if __name__ == "__main__":
    monitor_airline_tweets()
