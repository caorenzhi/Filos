import tweepy
from DB import DB
import yaml,os
import time
import sys
import datetime
from DatabaseOp_SQL import DatabaseOperation
import pandas as pd
import numpy as np

class TweetCraw():
    '''
    This class can be used to craw data from Twitter, either in a way of searching or streaming
    '''
    def __init__(self, config_file = None, twitter_config_file = None):
        #self.config = yaml.safe_load(open(os.path.join("config", 'config.yml')))
        #self.db_pardir = os.path.dirname(os.path.join(*self.config['db_file_path']))
        currentDIR = os.path.dirname(os.path.abspath(__file__))

        if config_file == None:
            self.config = yaml.safe_load(open(os.path.join(currentDIR, "../config", 'config.yml')))
        else:
            self.config = yaml.safe_load(open(config_file))

        self.tweet_DB = os.path.join(currentDIR,"..",*self.config['twitter_db_file_path'])

        if twitter_config_file == None:
            self.twitter_config = yaml.safe_load(open(os.path.join(currentDIR, "../config", 'twitter.yml')))
        else:
            self.twitter_config = yaml.safe_load(open(twitter_config_file))

        self.consumer_key=self.twitter_config['CONSUMER_KEY']
        self.consumer_secret=self.twitter_config['CONSUMER_SECRET']
        self.bearer_token=self.twitter_config['BEARER_TOKEN']
        self.access_token=self.twitter_config['ACCESS_TOKEN']
        self.access_token_secret=self.twitter_config['ACCESS_TOKEN_SECRET']

        self.client=tweepy.Client(bearer_token=self.bearer_token,consumer_key=self.consumer_key,consumer_secret=self.consumer_secret,
                                  access_token=self.access_token,access_token_secret=self.access_token_secret,wait_on_rate_limit=True)



        self.tweet_op = DatabaseOperation(userName= "database user name", passWord= "your database password", dataBase = "your database name")    # only need to initialize once, in the future, this should be moved to config

    def getTwitterID(self, twitterName):
        """
        This function will return None if the twitter name is not found, otherwise, it will return a 64 bit int ID
        """
        twitterName = twitterName.strip()
        if len(twitterName) < 1:
            return None
        if twitterName[0] == "@":
            twitterName = twitterName[1:]    # we don't need the @ at the beginning
        try:
            ID  = api.get_user( username=twitterName).data.id    # this is the new version of twitter, old version, use api.get_user( screen_name=twitterName).id
            return ID
        except:
            try:
                ID = api.get_user( screen_name=twitterName).id
                return ID
            except:
                return None    # now found



    def search_tweets(self,clientmethod,query,start_time=None, end_time=None,max_results=10,limit=100):
        count=0
        while count<3:
            try:
                tweepyResult = tweepy.Paginator(clientmethod, query=query, expansions=['author_id'],
                                                 tweet_fields=['created_at', 'public_metrics', 'entities'],
                                                 user_fields=['public_metrics'],
                                                 max_results=max_results, limit=limit, start_time=start_time,
                                                 end_time=end_time)
                if tweepyResult is None:
                    return 0
                #print(tweepyResult)
                for response in tweepyResult:
                    if response is None or len(response) == 0:
                        continue
                    #print("now checking the response:")
                    #print(response)
                    tweets_list = response[0]
                    #print(tweets_list)
                    if tweets_list is None or len(tweets_list) == 0:
                        continue
                    users = {u["id"]: u for u in response.includes['users']}
                    #print("get users:" + str(users))
                    for tweet in tweets_list:
                        if tweet is None or len(tweet) == 0:
                            continue
                        #print("!!! check tweet:")
                        #print(tweet)
                        data = {}
                        topiclist = []
                        if users[tweet.author_id]:
                            user = users[tweet.author_id]
                        data['tweet_id'] = tweet.id
                        data['tweet_text'] = tweet.text
                        #print("!! the hashtag is ")
                        #print(tweet.entities)

                        if tweet.entities is not None and 'hashtags' in tweet.entities and tweet.entities['hashtags'] is not None:
                            #print("!Cautions: checking ")
                            #print(tweet.entities['hashtags'])
                            for topic in tweet.entities['hashtags']:
                                topiclist.append(topic['tag'])
                            data['tweet_topic'] = ' '.join(word for word in topiclist)
                        else:
                            data['tweet_topic'] = query

                        #print("Done, now saving everything into database ")
                        data['create_at'] = tweet.created_at
                        data['insert_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        data['like_count'] = tweet.public_metrics['like_count']
                        data['reply_count'] = tweet.public_metrics['reply_count']
                        data['retweet_count'] = tweet.public_metrics['retweet_count']
                        data['quote_count'] = tweet.public_metrics['quote_count']
                        data['user_id'] = user.id
                        data['username'] = user.username
                        data['followers_count'] = user.public_metrics['followers_count']
                        data['following_count'] = user.public_metrics['following_count']
                        data['tweet_count'] = user.public_metrics['tweet_count']
                        self.tweet_op = DatabaseOperation(userName= "database user name", passWord= "your database password", dataBase = "your database name", Twitterdata=data)
                        self.tweet_op.insertTwitter_db()
                    time.sleep(2)
                count+=3   # we are done with three times
            except Exception as inst:
                print('Exception occurs' + str(inst))
                time.sleep(60)
                count+=1         # we will try 2 more times

    def get_retweeters(self,tweet_id):
        users=self.client.get_retweeters(id=tweet_id)
        return users

    '''
    A new user registration
    '''
    def register_user(self, user_id, user_name, source_from):
        results = self.tweet_op.searchTwitter_db("select * from userinfo where user_id = \"" + str(user_id) + "\";")
        if results != []:
            print("user has existed")
        else:
            try:
                self.tweet_op.insertTwitter("INSERT INTO userinfo(user_id,name,source_from) VALUES (%s,%s,%s);",
                                       params=(user_id, user_name, source_from))
                print('register succeed')
            except Exception as inst:
                print('Exception occurs' + str(inst))

    '''
    make user subscribe a twitter account
    '''
    def make_subsription(self,user_id,twitter_account):
        results = self.tweet_op.searchTwitter_db("select * from userinfo where user_id = \"" + str(user_id) + "\";")

        if results==[]:
            print("User not exist, you need to first register the service")
            return 3
        else:
            user = self.client.get_user(username=twitter_account)
            if user.data == None:
                print("The input twitter_account doesn't exist")
            else:
                try:
                    ### Need to update twitterSub table, first check if the twitter ID is already in the table
                    # if not, insert the twitter id and name
                    # otherwise, update the twitter id and name
                   # ????

                    SubscribeTwitterID = user.data.id
                    SubscribeTwitterName = user.data.username
                    values=self.tweet_op.searchTwitter_db("select * from subscription where user_id =  \""+str(user_id)+"\"" + " and SubscribeTwitterID = \""+str(SubscribeTwitterID) + "\";")
                    if values==[]:
                        # need to add the twitter info into subscription table and twitterSub table
                        self.tweet_op.insertTwitter("INSERT INTO subscription VALUES (%s,%s);",params=(results[0][0],SubscribeTwitterID))
                        query = self.tweet_op.searchTwitter_db(
                            "select * from twitterSub where SubscribeTwitterID =  \"" + str(SubscribeTwitterID) + "\";")
                        if query==[]:
                            print('Add new twitter account into twitterSub')
                            self.tweet_op.insertTwitter("INSERT INTO twitterSub VALUES (%s,%s);",
                                                    params=(SubscribeTwitterID,SubscribeTwitterName))
                        else:
                            print("We have this account in database")
                            print(query[0][1])
                            if query[0][1]==SubscribeTwitterName:
                                print('database name are the same as input')
                            else:
                                print('update the twitterSub')
                                self.tweet_op.insertTwitter("UPDATE twitterSub set SubscribeTwitterName = "+"\""+str(SubscribeTwitterName)+"\""+" where SubscribeTwitterID = "+"\""+str(SubscribeTwitterID)+"\";")
                        print("subscription succeed!")
                        print('now update the subscription_tweets database')
                        self.getUserTimeline(SubscribeTwitterID=SubscribeTwitterID)
                    else:
                        print('you have already subscribed it')
                except Exception as inst:
                    print('Exception occurs' + str(inst))

    '''
    make user unsubscribe a twitter account
    '''
    def delete_subsription(self,user_id,twitter_account):
        #????
      #  update the user_name to user_id now, update the following code:

        results = self.tweet_op.searchTwitter_db("select * from userinfo where user_id = \"" + str(user_id) + "\";")
        if results == []:
            print("User not exist, you need to first register the service")
        else:
            user = self.client.get_user(username=twitter_account)
            if user.data == None:
                raise ValueError("The input twitter_account doesn't exist")
            else:
                try:
                    SubscribeTwitterID = user.data.id
                    SubscribeTwitterName = user.data.username
                    values = self.tweet_op.searchTwitter_db(
                        "select * from subscription where user_id =  \"" + str(user_id) + "\"" + " and SubscribeTwitterID = \"" + str(SubscribeTwitterID) + "\";")
                    if values == []:
                        print("You don't subscribe this account")
                    else:
                        self.tweet_op.deleteTwitter_db(sqlStatement="delete from subscription where user_id =  \""+str(user_id)+"\"" + " and SubscribeTwitterID = \""+str(SubscribeTwitterID) + "\";")
                        query=self.tweet_op.searchTwitter_db(
                            "select * from subscription where SubscribeTwitterID =  \"" + str(SubscribeTwitterID) + "\";")
                        if query==[]:
                            print("delete this twitter account in the twitterSub")
                            self.tweet_op.deleteTwitter_db(
                                sqlStatement="delete from twitterSub where SubscribeTwitterID = \"" + str(SubscribeTwitterID) + "\";")
                        print("unsubscription succeed!")
                except Exception as inst:
                    print('Exception occurs' + str(inst))

    '''
    update the subscription_tweets table
    '''
    def update_subscription_tweet(self):
        results = self.tweet_op.searchTwitter_db("select SubscribeTwitterID from twitterSub;")
        results = [item[0] for item in results]
        # get all users' following twitter accounts
        results = np.unique(results)
        for twitter_id in results:
            self.getUserTimeline(SubscribeTwitterID=twitter_id)


    '''
    get one user's followings twitter account from the database.
    '''
    def getfollowing_users(self,user_id):
        SubscribeTwitterIDs=self.tweet_op.searchTwitter_db("select SubscribeTwitterID from subscription where user_id = \""+user_id+"\";")
        if len(SubscribeTwitterIDs) != 0:
            SubscribeTwitterIDs = [id[0] for id in SubscribeTwitterIDs]
        if len(SubscribeTwitterIDs) == 0:
            print('User have no subscription')

        return SubscribeTwitterIDs


    '''
    download one user tweets into the database. First search the database, if exists,
    then download most recent tweets. If not exists, then download all the historical tweets.
    '''
    def getUserTimeline(self, SubscribeTwitterID):
        results=self.tweet_op.SQL_to_dataframe("select * from subscription_tweets where SubscribeTwitterID = "+str(SubscribeTwitterID)+";")
        if results.empty:
            self.getUserHistoryTimeline(SubscribeTwitterID)
        else:
            max_id=max(results['tweet_id'])
            self.getUserHistoryTimeline(SubscribeTwitterID,since_id=max_id)

    '''
    get one user recent tweets(specify the time)
    '''
    def get_user_recent_tweets(self,SubscribeTwitterID,days=1):
        self.getUserTimeline(SubscribeTwitterID)
        results = self.tweet_op.SQL_to_dataframe(
            "select * from subscription_tweets where SubscribeTwitterID = " + str(SubscribeTwitterID) + ";")
        SubscribeTwitterName=self.tweet_op.searchTwitter_db("select SubscribeTwitterName from twitterSub where SubscribeTwitterID = \""+str(SubscribeTwitterID)+"\";")
        results['create_at']= pd.to_datetime(results['create_at'])
        results['SubscribeTwitterName']=SubscribeTwitterName[0][0]
        starttime=datetime.datetime.now()-datetime.timedelta(days=days)
        results=results[results['create_at']>starttime]
        results=results.sort_values(by='create_at', ascending=False)
        return results

    '''
    get one user all the historical tweets(exclude replies and retweet)
    '''
    def getUserHistoryTimeline(self,SubscribeTwitterID,since_id=None,start_time=None):
        count = 0
        while count < 3:
            try:
                tweepyResult = tweepy.Paginator(self.client.get_users_tweets, id=SubscribeTwitterID,expansions=['author_id','attachments.media_keys'],tweet_fields=['created_at', 'public_metrics', 'entities'],
                                                    exclude=['replies','retweets'],max_results=100,user_fields=['public_metrics'],media_fields=['type','url','preview_image_url'],since_id=since_id)
                if tweepyResult is None:
                    print("there is no tweepyResult")
                    return 0
                # print(tweepyResult)
                for response in tweepyResult:

                    if response is None or len(response) == 0:
                        print("there is no response")
                        return 0
                    # print("now checking the response:")
                    # print(response)
                    tweets_list = response[0]
                    if 'media' in response.includes:
                        medias = response.includes['media']
                    else:
                        medias = None
                    # print(tweets_list)
                    if tweets_list is None or len(tweets_list) == 0:
                        print("there is no tweets_list")
                        count += 3
                        break
                    users = {u["id"]: u for u in response.includes['users']}
                    # print("get users:" + str(users))
                    for tweet in tweets_list:
                        if tweet is None or len(tweet) == 0:
                            print("there is no tweet")
                            break
                        data = {}
                        topiclist = []
                        if users[tweet.author_id]:
                            user = users[tweet.author_id]
                        data['tweet_id'] = tweet.id
                        data['tweet_text'] = tweet.text

                        if tweet.entities is not None and 'hashtags' in tweet.entities and tweet.entities[
                            'hashtags'] is not None:
                            for topic in tweet.entities['hashtags']:
                                topiclist.append(topic['tag'])
                            data['tweet_topic'] = ' '.join(word for word in topiclist)
                        else:
                            data['tweet_topic'] = None

                        # print("Done, now saving everything into database ")
                        data['create_at'] = tweet.created_at
                        data['insert_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        data['like_count'] = tweet.public_metrics['like_count']
                        data['reply_count'] = tweet.public_metrics['reply_count']
                        data['retweet_count'] = tweet.public_metrics['retweet_count']
                        data['quote_count'] = tweet.public_metrics['quote_count']
                        data['SubscribeTwitterID'] = user.id
                        # data['SubscribeTwitterName'] = user.username
                        data['followers_count'] = user.public_metrics['followers_count']
                        data['following_count'] = user.public_metrics['following_count']
                        data['tweet_count'] = user.public_metrics['tweet_count']

                        if tweet.attachments != None:
                            mediakey = tweet.attachments['media_keys'][0]
                            if medias==None:
                                data['media_key'] = None
                                data['media_type'] = None
                                data['media_url'] = None
                                data['media_preview_url'] = None
                            else:
                                for media in medias:
                                    if media.media_key == mediakey:
                                        data['media_key']=media.media_key
                                        data['media_type']=media.type
                                        data['media_url']=media.url
                                        data['media_preview_url']=media.preview_image_url
                        else:
                            data['media_key'] = None
                            data['media_type'] = None
                            data['media_url'] = None
                            data['media_preview_url'] = None
                        self.tweet_op.insertTwitter_supscrip_db(Twittersubsripdata=data)
                    time.sleep(2)
                count += 3
            except Exception as inst:
                print('Exception occurs' + str(inst))
                time.sleep(60)
                count += 1  # we will try 2 more times



# if use basic developer account, call this class
class TweetSimpleSearch(TweetCraw):

    def __init__(self, config_file=None, twitter_config_file=None):
        super(TweetSimpleSearch,self).__init__(config_file,twitter_config_file)

    # search the recent 7 days tweets given the query, and store the data to the database
    def search_tweet7days(self,query,start_time=None, end_time=None,max_results=10,limit=100):
        self.search_tweets(self.client.search_recent_tweets, query, start_time, end_time, max_results, limit)




# if use academic developer account, call this class
class TweetAcademicSearch(TweetCraw):

    def __init__(self, config_file=None, twitter_config_file=None):
        super(TweetAcademicSearch,self).__init__(config_file,twitter_config_file)

    # search the full archive of tweets given the query, and store the data to the database
    # need to use academic account
    def search_tweet_from_archive(self, query,start_time=None, end_time=None,max_results=10,limit=100):
        self.search_tweets(self.client.search_all_tweets, query, start_time, end_time,
                           max_results, limit)
