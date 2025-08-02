import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def get_twitter_client():
    with open('airflow/twitter_dags/x_credentials.json') as f:
        credentials = json.load(f)

    client = tweepy.Client(
        bearer_token=credentials['bearer_token'],
        consumer_key=credentials['api_key'],
        consumer_secret=credentials['api_secret_key'],
        access_token=credentials['access_token'],
        access_token_secret=credentials['access_token_secret']
    )
    return client

def get_tweets(client):
    try:
        response = client.search_recent_tweets(
            query='from:elonmusk',
            tweet_fields=['created_at', 'public_metrics'],
            max_results=100
        )

        tweets = []
        for tweet in response.data:
            tweets.append({
                'id': tweet.id,
                'text': tweet.text,
                'created_at': tweet.created_at,
                'retweet_count': tweet.public_metrics['retweet_count'],
                'like_count': tweet.public_metrics['like_count']
            })
        df = pd.DataFrame(tweets)
        df.to_csv('airflow/twitter_dags/tweets.csv', index=False)
    except tweepy.TweepyException as e:
        print(f"Error fetching tweets: {e}")
        df = pd.read_csv('airflow/twitter_dags/tweets.csv')
    finally:
        return df

def run_twitter_etl():
    client = get_twitter_client()
    df = get_tweets(client)
    df.to_csv('s3://airflow-x-etl-bucket/tweets.csv', index=False)


if __name__ == "__main__":
    run_twitter_etl()