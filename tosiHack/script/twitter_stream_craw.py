# this is the main script and start everything, download all data
# download the twitter
# download the stock
# download other data
import argparse
from twitter_craw import *
from twitter_stream import *
import multiprocessing
import datetime
from DatabaseOp_SQL import DatabaseOperation

if __name__=='__main__':

    currentDIR = os.path.dirname(os.path.abspath(__file__))
    twitterYML = currentDIR + "/../config/twitter.yml"


    twitteracademic=currentDIR + "/../config/twitter_academic.yml"
    rule1 = '#Bitcoin lang:en -is:retweet'


    config = yaml.safe_load(open(os.path.join(currentDIR, "../config", 'config.yml')))
    #print("Using configuration: " + twitterYML)
    tweetdb = os.path.join(currentDIR, "..", *config['twitter_db_file_path'])
    parser = argparse.ArgumentParser(description='Arguments for the live tweet streaming')

    #parser.add_argument('--max_results', type=int, help="number of tweets we need", default=1000000)
    parser.add_argument('--rule', type=str, help="query content,format data:#Bitcoin lang:en -is:retweet",default=rule1)
    parser.add_argument('--path', type=str, help="twitter_api_path, format: twitter.yml",
                        default=twitterYML)
    args = parser.parse_args()

    # check the number of tweets in mysql
    db = DatabaseOperation(userName= "mysql user name", passWord= "mysql pass", dataBase = "mysql database")
    values = db.searchTwitter_db(sqlStatement="select * from tweets")
    if values==None:
        print(0)
    else:
        print('the number of tweets in database')
        print(len(values))

    if args.path==twitterYML:
        twitter_path=args.path
    else:
        twitter_path=currentDIR + "/../config/"+args.path

    tweet_stream=TweetStream(twitter_config_file=twitter_path)
    tweet_stream.use_stream(rule=args.rule)
    # values = db.searchTwitter_db(sqlStatement="select * from tweets")
    # print(len(values))

