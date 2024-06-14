import json
from prefect import task, flow
from kafka import KafkaConsumer

from pymongo import MongoClient
from pymongo.server_api import ServerApi


AIRLINE_URL = "http://localhost:8000"

KAFKA_TOPIC_AIRLINES = "raw_airline_tweet"

KAFKA_SERVERS='wanted-mongrel-13316-eu1-kafka.upstash.io:9092' 
SASL_MECHANISM='SCRAM-SHA-256'
SECURITY_PROTOCOL='SASL_SSL'
SASL_PLAIN_USERNAME='d2FudGVkLW1vbmdyZWwtMTMzMTYkDwjeNJ-sLOhmEfWrjidKnl7qy5_44KjNSkw'
SASL_PLAIN_PASSWORD='YzViM2ZjYjgtNDE3OS00ZmRhLWJhYmEtZTYyMjM2OGUyMjJh'

MONGO_URI = "mongodb+srv://adamhill:Prefect-falcon-pallas@prefect-tutorial.haryygz.mongodb.net/?retryWrites=true&w=majority&appName=Prefect-tutorial"

def get_mongo_db(uri: str = MONGO_URI):
    client = MongoClient(uri,server_api=ServerApi('1'))
    return client

def get_kafka_consumer(kafka_topic: str):
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

@task
def write_msg_to_mongo(
    record: dict,
    client: MongoClient,
    ):
    db = client["Prefect-tutorial"]
    collection = db["raw_airline_tweets"]
    collection.insert_one(record)
    

@task
def consume_airline_tweets(kafka_topic = KAFKA_TOPIC_AIRLINES):
    consumer = get_kafka_consumer(kafka_topic)
    client = get_mongo_db()
    while True:
        poll_result = consumer.poll(timeout_ms=5000)
        for context, messages in poll_result.items():
            for msg in messages:
                result = write_msg_to_mongo(msg.value, client=client)
                print(msg.value)
                print(result)

@flow
def monitor_airline_tweets(log_prints=True):
    consume_airline_tweets(KAFKA_TOPIC_AIRLINES)
    

if __name__ == "__main__":
    monitor_airline_tweets()
