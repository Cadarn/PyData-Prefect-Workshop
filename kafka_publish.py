import json
from prefect import task, flow
from kafka import KafkaProducer
import httpx


AIRLINE_URL = "http://localhost:8000"

KAFKA_TOPIC = "raw_airline_tweet"

KAFKA_SERVERS='wanted-mongrel-13316-eu1-kafka.upstash.io:9092' 
SASL_MECHANISM='SCRAM-SHA-256'
SECURITY_PROTOCOL='SASL_SSL'
SASL_PLAIN_USERNAME='d2FudGVkLW1vbmdyZWwtMTMzMTYkDwjeNJ-sLOhmEfWrjidKnl7qy5_44KjNSkw'
SASL_PLAIN_PASSWORD='YzViM2ZjYjgtNDE3OS00ZmRhLWJhYmEtZTYyMjM2OGUyMjJh'

@task
def publish_to_kafka(json_messages, kafka_topic):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        sasl_mechanism=SASL_MECHANISM,
        security_protocol=SECURITY_PROTOCOL,
        sasl_plain_username=SASL_PLAIN_USERNAME,
        sasl_plain_password=SASL_PLAIN_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for message in json_messages:
        producer.send(kafka_topic, value=message)
    producer.flush()


@task(retries=5, retry_delay_seconds=1)
def fetch_airline_tweet(url: str)->dict:
    message = httpx.get(url)
    message.raise_for_status()
    data = message.json()
    if not isinstance(data, list):
        data = [data]
    return data


@flow
def stream_airline_tweet_to_kafka():
    url = AIRLINE_URL + "/get_tweet"
    tweet = fetch_airline_tweet(url)
    publish_to_kafka(tweet, KAFKA_TOPIC)

if __name__ == "__main__":
    stream_airline_tweet_to_kafka()
