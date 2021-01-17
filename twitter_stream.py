from flask import Flask
from confluent_kafka import Producer, KafkaError, Consumer
import tweepy
import requests
import os
import json


app = Flask(__name__)
app.config.from_pyfile('envvar.py')

# Confluent Secret Keys
confluent_servers = app.config.get("CONFLUENT_SERVERS")
confluent_username = app.config.get("CONFLUENT_USERNAME")
confluent_password = app.config.get("CONFLUENT_PASSWORD")

# Create Producer instance
producer = Producer({
    'bootstrap.servers': confluent_servers,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': confluent_username,
    'sasl.password': confluent_password,
})

conf = {
    'bootstrap.servers': confluent_servers,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': confluent_username,
    'sasl.password': confluent_password,
}


# Twitter API set-up
consumer_key = app.config.get("TWITTER_API_KEY")
consumer_secret = app.config.get("TWITTER_API_KEY_SECRET")
bearer_token = app.config.get("TWITTER_BEARER")
# If need to change callback, update it in Twitter Developer Portal as well
callback = 'http://127.0.0.1:5000/callback'
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)


def acked(err, msg):
    """Delivery report handler called on
        successful or failed delivery of message
        """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers, delete, bearer_token, topic="cat"):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "{} has:images".format(
            topic), "tag": "{} pictures".format(topic)},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers, set):
    params = {
        "tweet.fields": "created_at",
        'expansions': 'attachments.media_keys',
        'media.fields': 'preview_image_url'
    }
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True, params=params,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    # ccloud_lib.create_topic(conf, 'cat')
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            record_value = json.dumps(json_response)
            print(json.dumps(json_response, indent=4, sort_keys=True))

            producer.produce('cat', key='cat',
                             value=record_value, on_delivery=acked)
        producer.flush()


if __name__ == '__main__':
    topic = 'cat'
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set1 = set_rules(headers, delete, bearer_token, topic)
    get_stream(headers, set1)
