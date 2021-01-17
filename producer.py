#!/usr/bin/env python

from flask import Flask
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import tweepy

app = Flask(__name__)
app.config.from_pyfile('envvar.py')
delivered_records = 0
topic = "topic3"

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


# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
        successful or failed delivery of message
        """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    for n in range(10):
        record_key = "travis"
        record_value = json.dumps({'count': n})
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key,
                         value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
