from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pykafka import KafkaClient
# from kafka import Kaf
import dotenv
import json
import os


def get_kafka_client():
    return KafkaClient(hosts='localhost:9092')


class StdOutListener(StreamListener):
    def on_data(self, raw_data):
        client = get_kafka_client()
        topic = client.topics['twitterdata']
        producer = topic.get_sync_producer()
        producer.produce(raw_data.encode('ascii'))
        print(raw_data)
        return True

    def on_error(self, status_code):
        print(status_code)


if __name__ == '__main__':
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
    dotenv_path = os.path.join(project_dir, '.env')
    if dotenv.load_dotenv(dotenv_path):
        cred = dotenv.dotenv_values()
        auth = OAuthHandler(cred.get('TWI_CONSUMER_KEY'), cred.get('TWI_CONSUMER_SECRET'))
        auth.set_access_token(cred.get('TWI_ACCESS_TOKEN'), cred.get('TWI_ACCESS_TOKEN_SECRET'))
        listener = StdOutListener()
        stream = Stream(auth, listener)
        stream.filter(track=['covid'])
