from flask import Flask
import tweepy
import requests
import os
import json

app = Flask(__name__)
app.config.from_pyfile('envvar.py')

# Twitter API set-up
consumer_key = app.config.get("TWITTER_API_KEY")
consumer_secret = app.config.get("TWITTER_API_KEY_SECRET")
bearer_token = app.config.get("TWITTER_BEARER")
# If need to change callback, update it in Twitter Developer Portal as well
callback = 'http://127.0.0.1:5000/callback'
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)


def get_tweets():
    # Collect tweets
    tweets = tweepy.Cursor(api.search,
                           q="nfl",
                           lang="en",
                           since='2021-01-15').items(2)

    # Iterate and print tweets
    for tweet in tweets:
        print(tweet.text)

    return tweet


if __name__ == '__main__':
    get_tweets()
