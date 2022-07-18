import tweepy
from SQL_DB import SQL_DB
import yaml,os
import time

class DatabaseOperation(SQL_DB):
    '''
    This is a class used to operate the tweets table and other tables in Database based on the DB class in DB.py
    '''

    def __init__(self,db_config = None, userName = None, passWord = None, dataBase = None, initializeTable = False, Twitterdata=None, Stockdata =None):
        super(DatabaseOperation, self).__init__(db_config, userName, passWord, dataBase, initializeTable)
        if Twitterdata!=None:
            self.tweet_id=Twitterdata['tweet_id']
            self.tweet_text=Twitterdata['tweet_text']
            self.tweet_topic=Twitterdata['tweet_topic']
            self.create_at=Twitterdata['create_at']
            self.insert_at=Twitterdata['insert_at']
            self.like_count=Twitterdata['like_count']
            self.reply_count=Twitterdata['reply_count']
            self.retweet_count=Twitterdata['retweet_count']
            self.quote_count=Twitterdata['quote_count']
            self.user_id=Twitterdata['user_id']
            self.username=Twitterdata['username']
            self.followers_count=Twitterdata['followers_count']
            self.following_count=Twitterdata['following_count']
            self.tweet_count=Twitterdata['tweet_count']



    # if twitter data is not None, we can insert the instance into table
    def insertTwitter_db(self):
        self.executeSQL(query="INSERT ignore INTO tweets VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",params=(self.tweet_id, self.tweet_text, self.tweet_topic, self.create_at,self.insert_at, self.like_count, self.reply_count,self.retweet_count, self.quote_count, self.user_id, self.username, self.followers_count,self.following_count,self.tweet_count))


    def insertTwitter_supscrip_db(self,Twittersubsripdata=None):

        if Twittersubsripdata != None:
            self.SubscribeTwitterID=Twittersubsripdata['SubscribeTwitterID']
            # self.SubscribeTwitterName=Twittersubsripdata['SubscribeTwitterName']
            self.tweet_id = Twittersubsripdata['tweet_id']
            self.tweet_text = Twittersubsripdata['tweet_text']
            self.tweet_topic = Twittersubsripdata['tweet_topic']
            self.create_at = Twittersubsripdata['create_at']
            self.insert_at = Twittersubsripdata['insert_at']
            self.like_count = Twittersubsripdata['like_count']
            self.reply_count = Twittersubsripdata['reply_count']
            self.retweet_count = Twittersubsripdata['retweet_count']
            self.quote_count = Twittersubsripdata['quote_count']
            self.followers_count = Twittersubsripdata['followers_count']
            self.following_count = Twittersubsripdata['following_count']
            self.tweet_count = Twittersubsripdata['tweet_count']
            self.media_key=Twittersubsripdata['media_key']
            self.media_type=Twittersubsripdata['media_type']
            self.media_url=Twittersubsripdata['media_url']
            self.media_preview_url=Twittersubsripdata['media_preview_url']

        self.executeSQL(query="INSERT ignore INTO subscription_tweets(SubscribeTwitterID,tweet_id,tweet_text,tweet_topic,create_at,insert_at,like_count,reply_count,retweet_count,quote_count,media_key,media_type,media_url,media_preview_url,followers_count,following_count,tweet_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",params=(self.SubscribeTwitterID,self.tweet_id, self.tweet_text, self.tweet_topic, self.create_at,self.insert_at, self.like_count, self.reply_count,self.retweet_count, self.quote_count,self.media_key,self.media_type,self.media_url,self.media_preview_url,self.followers_count,self.following_count,self.tweet_count))

    def insertTwitter(self,sqlStatement,params=None):
        self.executeSQL(query=sqlStatement,params=params)

    def searchTwitter_db(self,sqlStatement):
        results=self.executeSQL(sqlStatement)
        return results

    def searchTwitter_store_dataframe(self,sqlStatement):
        results=self.SQL_to_dataframe(sqlStatement)
        return results

    def deleteTwitter_db(self,sqlStatement):
        self.executeSQL(sqlStatement)

    def insertStock_db(self,sqlStatement,data):
        self.executemanySQL(sqlStatement,data)

    def searchStock_db(self,sqlStatement):
        results=self.executeSQL(sqlStatement)
        return results
