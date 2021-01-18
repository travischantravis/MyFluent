from twitter_stream import create_headers, get_rules, set_rules, delete_all_rules, get_stream
from twitter_tweets import get_tweets
from flask import Flask, render_template, redirect, request, session, Response, stream_with_context
from flask_socketio import SocketIO, emit, disconnect
from confluent_kafka import Producer, KafkaError, Consumer
from datetime import datetime, timedelta
import timeago
import json
import ccloud_lib
import tweepy
import requests
import eventlet
# eventlet.monkey_patch()


# Import others .py files

app = Flask(__name__)
app.config.from_pyfile('envvar.py')
socketio = SocketIO(app)
delivered_records = 0
topic = 'topic3'


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

# Create Consumer instance
consumer = Consumer({
    'bootstrap.servers': confluent_servers,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': confluent_username,
    'sasl.password': confluent_password,
    'group.id': 'python_example_group_1',
    # earliest or latest
    'auto.offset.reset': 'latest',
})

# Twitter API set-up
consumer_key = app.config.get("TWITTER_API_KEY")
consumer_secret = app.config.get("TWITTER_API_KEY_SECRET")
bearer_token = app.config.get("TWITTER_BEARER")
# If need to change callback, update it in Twitter Developer Portal as well
callback = 'http://127.0.0.1:5000/callback'
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)


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


def kafka_consume():
    # Subscribe to topic
    consumer.subscribe(['cat'])

    print("consuming...")
    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


@app.route('/')
def home_page():
    return render_template('index.html', delivered_records=delivered_records)


def stream_template(template_name, **context):
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.enable_buffering(5)
    return rv


@app.route('/stream')
def stream_page():
    return render_template('stream.html')


@socketio.event
def produce_event(data, topic="cat"):
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set1 = set_rules(headers, delete, bearer_token, topic)
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
                response.status_code, response.text)

        )

    repetition = 0
    emit('produce_response', {
        'data': 'Producing tweets to Confluent...'})
    for response_line in response.iter_lines():
        if repetition < 10:
            repetition += 1
            if response_line:
                json_response = json.loads(response_line)
                record_value = json.dumps(json_response)
                producer.produce('cat', key='cat',
                                 value=record_value, on_delivery=acked)
            producer.flush()


@socketio.event
def stream_event(data):
    topic = 'cat'
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set1 = set_rules(headers, delete, bearer_token, topic)
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
                response.status_code, response.text)

        )

    repetition = 0
    for response_line in response.iter_lines():
        if repetition < 10:
            repetition += 1
            if response_line:
                json_response = json.loads(response_line)
                record_value = json.dumps(json_response)
                print(json.dumps(json_response, indent=4, sort_keys=True))
                emit('stream_response', {'data': record_value})
                # eventlet.sleep(1)


@app.route('/my_producer')
def my_producer():
    # record the event synchronously
    # calling flush() guarantees that each event is written to Kafka before continuing
    for n in range(5):
        record_value = json.dumps({'count': n})
        producer.produce(topic, key="travis",
                         value=record_value, on_delivery=acked)
    producer.flush()
    print(delivered_records)
    return render_template('index.html', delivered_records="ok")


@app.route('/my_consumer')
def my_consumer():
    kafka_consume()
    return "ok"
    # return render_template('index.html', delivered_records="ok")


@app.route("/dashboard", methods=['POST', 'GET'])
def dashboard():
    topic = "cat"
    if request.method == 'POST':
        topic = request.form['topic']

    print("Topic:", topic)
    tweets_unformatted1 = api.search(
        q="{} filter:media".format(topic), count=4, result_type='mixed', include_entities=True)

    tweets_formatted1 = []

    # Time zone difference
    hours_added = timedelta(hours=8)
    for tweet in tweets_unformatted1:
        temp = {
            "text": tweet.text.rsplit(' ', 1)[0],
            "created_at": timeago.format(tweet.created_at-hours_added, datetime.now()),
            "retweet_count": tweet.retweet_count,
            "favorite_count": tweet.favorite_count,
            "entities": tweet.entities,
            "user": tweet.user
        }
        tweet2 = tweet._json
        entities2 = tweet2.get('entities')
        if entities2 != None:
            media2 = entities2.get('media')
            if media2 != None:
                print(type(media2))
                # print(media2[0])
                for image in media2:
                    if 'media_url' in image:
                        print(image.get('media_url'))
                        media_url = image.get('media_url')
                        temp['media_url'] = media_url
            else:
                print("no media")
        else:
            print("no media")

        tweets_formatted1.append(temp)

    # right column

    # Streams
    # headers = create_headers(bearer_token)
    # rules = get_rules(headers, bearer_token)
    # delete = delete_all_rules(headers, bearer_token, rules)
    # set1 = set_rules(headers, delete, bearer_token, topic)
    # get_stream(headers, set1)

    return render_template('dashboard.html', tweets1=tweets_formatted1, topic=topic)


@socketio.on('my event')
def handle_json(data):
    print('received json: ')


if __name__ == '__main__':
    app.debug = "True"
    app.run()
    # socketio.run(app)
