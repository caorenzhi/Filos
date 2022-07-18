import tweepy
from SQL_DB import SQL_DB
import yaml,os
import time
import sys

from DatabaseOp_SQL import DatabaseOperation


class TweetStream(tweepy.StreamingClient):
    '''
    This class can be used to craw data from Twitter in real time
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

        self.bearer_token = self.twitter_config['BEARER_TOKEN']

        super(TweetStream, self).__init__(self.bearer_token, wait_on_rate_limit=True)

    def on_connect(self):
        print('twitter stream has been connected')

    def on_connection_error(self):
        print('stream connects error')

    def on_disconnect(self):
        print('stream disconnect')

    def on_exception(self, exception):
        print('An exception occurs')
        print(exception)

    def on_request_error(self, status_code):
        print('request error occurs')
        print(status_code)
        time.sleep(10)

    '''
    use stream to craw the live tweets
    '''
    def use_stream(self, rule):
        self.rule=rule
        self.count=0
        self.response_count=0
        rules_id=[v.id for v in self.get_rules()[0]]
        self.delete_rules(rules_id)
        self.add_rules(tweepy.StreamRule(rule))
        print(self.get_rules())
        self.filter(expansions=['author_id'], tweet_fields=['created_at', 'public_metrics', 'entities'],
                        user_fields=['public_metrics'],)


    '''
    collect the stream results to the database
    '''
    def on_response(self, response):
        if response == None:
            self.response_count+=1
            print('There is no response now')
            if self.response_count==10:
                self.disconnect()
        else:
            user = response[1]['users'][0]
            tweet = response[0]
            data = {}
            topiclist = []
            data['tweet_id'] = tweet.id
            data['tweet_text'] = tweet.text
            if tweet.entities is not None and 'hashtags' in tweet.entities and tweet.entities['hashtags'] is not None:
                # print("!Cautions: checking ")
                # print(tweet.entities['hashtags'])
                for topic in tweet.entities['hashtags']:
                    topiclist.append(topic['tag'])
                data['tweet_topic'] = ' '.join(word for word in topiclist)
            else:
                data['tweet_topic'] = self.rule

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
            while(True):
                try:
                    tweet_op = DatabaseOperation(userName= "your user name", passWord= "database password", dataBase = "database name", Twitterdata=data)
                    tweet_op.insertTwitter_db()
                    # values = tweet_op.searchTwitter_db(sqlStatement="select * from tweets")
                    # print(values)
                    break
                except Exception as e:
                    raise  e
                    print(e)
            self.count += 1
            # print('count is'+str(self.count))
            print(response)
            time.sleep(1)
            # if self.count > self.maxcount:
            #     self.disconnect()


    def get_retweeters(self,tweet_id):
        users=self.client.get_retweeters(id=tweet_id)
        return users
