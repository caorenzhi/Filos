# this is the main script and start everything, download all data
# download the twitter
# download the stock
# download other data
# this will use Academic account

from twitter_craw import *
from twitter_stream import *
import multiprocessing
import argparse
import datetime
if __name__=='__main__':
    currentDIR = os.path.dirname(os.path.abspath(__file__))
    twitterYML = currentDIR + "/../config/twitter_academic.yml"
    #print("Using configuration: " + twitterYML)
    query = '#Bitcoin lang:en -is:retweet'

    # take the end_time from tweet database
    config = yaml.safe_load(open(os.path.join(currentDIR, "../config", 'config.yml')))
    tweetdb = os.path.join(currentDIR, "..", *config['twitter_db_file_path'])
    db = DatabaseOperation(userName="database user name", passWord="your database password", dataBase="your database name")
    values = db.searchTwitter_db(sqlStatement="select create_at from tweets")
    print('number of tweets in the database')
    print(len(values))
    # take all the create_at time
    timelist = [x[0] for x in values]
    end_time = None
    start_time = None
    if timelist != []:
        end_time = min(timelist)
        print(end_time)
        end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        start_time = end_time - datetime.timedelta(days=30)
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    print('default start_time is :'+str(start_time))
    print('default end_time is :' + str(end_time))

    parser = argparse.ArgumentParser(description='Arguments for the search tweet function')
    parser.add_argument('--start_time', type=str, help="start_time format is YYYY-MM-DD HH:MM:SS",
                        default=start_time)
    parser.add_argument('--end_time', type=str, help="end_time format is YYYY-MM-DD HH:MM:SS", default=end_time)
    parser.add_argument('--query', type=str, help="query content,format reference:#Bitcoin lang:en -is:retweet", default=query)
    parser.add_argument('--max_results', type=int, help="max number of tweets in a response", default=400)
    parser.add_argument('--limit', type=int, help="number of response", default=20000)
    args = parser.parse_args()
    args.start_time = datetime.datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%dT%H:%M:%SZ")
    args.end_time = datetime.datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%dT%H:%M:%SZ")
    print('set start_time :'+args.start_time)
    print('set end_time :'+args.end_time)

    tweet_Academic = TweetAcademicSearch(twitter_config_file=twitterYML)
    tweet_Academic.search_tweet_from_archive(query=args.query, max_results=args.max_results, limit=args.limit,start_time=args.start_time,
                                             end_time=args.end_time)
