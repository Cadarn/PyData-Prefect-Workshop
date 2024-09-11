import sys
sys.path.append("..")

import json
from prefect import task, flow, get_run_logger
from kafka import KafkaProducer
import httpx

from config.kafka_config import KAFKA_SERVERS
AIRLINE_URL = "http://localhost:8000"
KAFKA_TOPIC = "raw_airline_tweet"

@task(name="Publish to Kafka", description="Publishes a list of JSON messages to a Kafka topic.")
def publish_to_kafka(json_messages: list[dict], kafka_topic: str):
    """
    Publishes a list of JSON messages to the specified Kafka topic.

    Args:
        json_messages (List[Dict]): A list of messages in JSON format to be published.
        kafka_topic (str): The Kafka topic to publish the messages to.

    Returns:
        None
    """
    logger = get_run_logger()
    logger.info(f"Publishing {len(json_messages)} messages to Kafka topic: {kafka_topic}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in json_messages:
        producer.send(kafka_topic, value=message)
    producer.flush()
    logger.info(f"Successfully published {len(json_messages)} messages to Kafka topic: {kafka_topic}")


@task(name="Fetch Airline Tweet", description="Fetches an airline tweet from the specified URL.", retries=5, retry_delay_seconds=1)
def fetch_airline_tweet(url: str) -> list[dict]:
    """
    Fetches an airline tweet from the specified URL.

    Args:
        url (str): The URL to fetch the tweet from.

    Returns:
        List[Dict]: A list containing the fetched tweet data.
    """
    logger = get_run_logger()
    logger.info(f"Fetching tweet from URL: {url}")
    
    response = httpx.get(url)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        data = [data]
    
    logger.info(f"Fetched tweet data: {data}")
    return data


@flow(name="Stream Airline Tweet to Kafka", description="Fetches airline tweets from an API and publishes them to a Kafka topic.")
def stream_airline_tweet_to_kafka(tweet_url: str, kafka_topic: str):
    """
    Fetches airline tweets from the API and publishes them to the Kafka topic at regular intervals.
    """
    logger = get_run_logger()
    logger.info(f"Starting stream from {tweet_url} to Kafka topic {kafka_topic}")

    tweet = fetch_airline_tweet(tweet_url)
    publish_to_kafka(tweet, kafka_topic)


if __name__ == "__main__":
    # creates a deployment and stays running to monitor for work instructions generated on the server
    # Edit the fields appropriately
    stream_airline_tweet_to_kafka.serve(name="Airline Tweets Stream to Kafka",
                      tags=["raw", "airline-api", "kafka"],
                      parameters={"tweet_url": AIRLINE_URL+"/get_tweet", "kafka_topic": KAFKA_TOPIC},
                      interval=15)