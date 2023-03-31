import tweepy
import time
import datetime
from datetime import *
import boto3
import json

api_key = ''
api_secret = ''
bearer_token = r""
access_token = ''
access_token_secret =''

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)


def sendToS3(lista, dateList):
    ACCESS_ID = ''
    ACCESS_KEY = r''

    s3 = boto3.client(
        's3',
        region_name='', #region here
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key= ACCESS_KEY
    )

    arq_name = min(dateList)

    boto3.client('s3').put_object(Key= '/'+str(arq_name)+'.json', Body=json.dumps(lista), Bucket = 'raw')


class MyStream(tweepy.StreamingClient):
    
    tweets = []
    datas = []
    
    def on_connect(self):
        print("conected")


    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:

            self.datas.append(datetime.strftime(datetime.strptime(str(tweet.created_at), "%Y-%m-%d %H:%M:%S%z"), "%Y-%m-%d %H %M %S"))

            self.tweets.append({"id" : tweet.id,
                                "tweet_text" : tweet.text,
                                "tweet_date" : datetime.strftime(datetime.strptime(str(tweet.created_at), "%Y-%m-%d %H:%M:%S%z"), "%Y-%m-%d %H:%M:%S")})

        if len(self.tweets) == 100:
            dados = self.tweets
            dateList = self.datas
            print("Create Json")
            sendToS3(dados,dateList)
            self.datas = []
            self.tweets = []


stream = MyStream(bearer_token=bearer_token)

search_terms = ["Bolsonaro", "presidente do brasil"]

for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields=['referenced_tweets','created_at'])