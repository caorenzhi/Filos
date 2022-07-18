#######################################################################################################
# Name: SQL_DB class
# Purpose: Initialize the database
# Input:
# Output:
# Developed by: Dr. Renzhi Cao
# Developed on: 1/15/2022
# Modified by:  7/10/2022
# Change log:
#######################################################################################################

import mysql.connector
from mysql.connector import errorcode
import sys
import json
import pandas as pd
import os
import logging
import datetime

today = datetime.datetime.now()
date_time = today.strftime("%m-%d-%Y")


# set up the logs, depends on where you run the script, logs would be in the Logs folder
LOG_DIR = "Logs"
if not os.path.isdir(LOG_DIR):
    os.system("mkdir "+LOG_DIR)

logFilePath = LOG_DIR+"/"+date_time+"-AINLP.log"
if not os.path.exists(logFilePath):
    open(logFilePath, 'w').close()

logging.basicConfig(filename= (logFilePath), level=logging.WARNING, format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s")

logger = logging.getLogger(__name__)

class SQL_DB:
    def __init__(self, db_config = None, userName = None, passWord = None, dataBase = None, initializeTable = False):
        '''
        Upon initialization, the DB_init class will connect to database
        You need to either set up db_config or userName and passWord
        '''
        if db_config is not None:
            ## Load configuration
            con_file = open(db_config)
            config = json.load(con_file)
            con_file.close()
            try:
                self.userName = config['user']
                self.passWord = config['pass']
                self.dataBase = config['database']
            except:
                self.errorMessage("Please set up user and pass and database in the configuration file "+db_config+", otherwise, we cannot connect to the DB")
        else:
            if userName is None or passWord is None or dataBase is None:
                self.errorMessage("If you don't provide the configuration file db_config, you must set up userName and passWord and database!")
            else:
                self.userName = userName
                self.passWord = passWord
                self.dataBase = dataBase


        ######################
        if initializeTable == True:
            #print("Warning, will drop the tables now")  we don't want to drop the table
            #self._dropAllTables()     # we will drop the table first and then do the rest, be careful here
            # now we will create the crypto basics tables for each crypto, the id will be unique for each crypto
            sql_command = "CREATE TABLE IF NOT EXISTS CoinmarketBasicTable (id INT NOT NULL PRIMARY KEY, name VARCHAR(100), symbol VARCHAR(30), slug VARCHAR(100), num_market_pairs INT, date_added DATETIME, tags VARCHAR(2000), max_supply double, circulating_supply double, total_supply double, platform VARCHAR(300), cmc_rank INT, self_reported_circulating_supply double, self_reported_market_cap double, last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"
            self.executeSQL(sql_command)

            # now we will create the price table for each crypto, the id is the same as the id in CoinmarketBasicTable
            sql_command = "CREATE TABLE IF NOT EXISTS CoinmarketPriceTable (priceID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, id INT, price double, volume_24h double, volume_change_24h double, percent_change_1h double, percent_change_24h double, percent_change_7d double, percent_change_30d double, percent_change_60d double, percent_change_90d double, market_cap double, market_cap_dominance double, fully_diluted_market_cap double, last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"
            self.executeSQL(sql_command)

            sql_command = ("CREATE TABLE IF NOT EXISTS tweets"
                           + "(tweet_id varchar(25) primary key,"
                           + "tweet_text text,"
                           + "tweet_topic text,"
                           + "create_at text,"
                           + "insert_at text,"
                           + "like_count INTEGER,"
                           + "reply_count INTEGER,"
                           + "retweet_count INTEGER,"
                           + "quote_count INTEGER,"
                           + "user_id VARCHAR(25),"
                           + "username varchar(25),"
                           + "followers_count integer,"
                           + "following_count integer,"
                           + "tweet_count integer);")
            print(sql_command)
            self.executeSQL(sql_command)

            sql_command = ("CREATE TABLE IF NOT EXISTS stocks"
                           + "(stock_id VARCHAR(30),"
                           + "price_time VARCHAR(30),"
                           + "low_price INTEGER,"
                           + "high_price INTEGER,"
                           + "open_price INTEGER,"
                           + "close_price INTEGER,"
                           + "volume INTEGER,"
                           + "insert_at text,"
                           + "primary key(stock_id,price_time));")
            print(sql_command)
            self.executeSQL(sql_command)

            # now we will create the User basics tables for each user
            sql_command = "CREATE TABLE IF NOT EXISTS userinfo (user_id varchar(25), name VARCHAR(100), source_from VARCHAR(30), primary key(user_id,source_from));"
            self.executeSQL(sql_command)

            # now we will create the Subscription table
            #sql_command = "CREATE TABLE IF NOT EXISTS subscription(user_id varchar(30), user_name VARCHAR(100), SubscribeTwitterID VARCHAR(64),SubscribeTwitterName VARCHAR(50), primary key(user_id,SubscribeTwitterID));"
            sql_command = "CREATE TABLE IF NOT EXISTS subscription(user_id varchar(25), SubscribeTwitterID VARCHAR(25), primary key(user_id,SubscribeTwitterID));"
            self.executeSQL(sql_command)

            # now we will create the subscription_tweets table
            sql_command = ("CREATE TABLE IF NOT EXISTS subscription_tweets"
                           + "(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                           + "SubscribeTwitterID VARCHAR(25),"
                           #+ "SubscribeTwitterName VARCHAR(50),"
                           + "tweet_id varchar(25),"
                           + "tweet_text text,"
                           + "tweet_topic text,"
                           + "create_at text,"
                           + "insert_at text,"
                           + "like_count INTEGER,"
                           + "reply_count INTEGER,"
                           + "retweet_count INTEGER,"
                           + "quote_count INTEGER,"
                           + "media_key varchar(30),"
                           + "media_type varchar(30),"
                           + "media_url varchar(30),"
                           + "media_preview_url varchar(30),"
                           + "followers_count integer,"
                           + "following_count integer,"
                           + "tweet_count integer);")
            print(sql_command)
            self.executeSQL(sql_command)

            # now we will create the twitterSub table, contains twitter ID and twitter name
            sql_command = "CREATE TABLE IF NOT EXISTS twitterSub(SubscribeTwitterID VARCHAR(64), SubscribeTwitterName VARCHAR(50), primary key(SubscribeTwitterID));"
            self.executeSQL(sql_command)

            #sys.exit(0)

            # if the time format is different, here is how we can do that:
            """
            SET @datestring = CAST('2022-06-14T21:35:00.000Z' AS DATETIME);
            insert INTO CoinmarketPriceTable VALUES( ..., @datestring)

            """
            logging.debug("Database Initialization is done. You only need to do it once.")


    def executeSQL(self, query,params=None):
        try:
            self.cnx = mysql.connector.connect(user=self.userName, password=self.passWord, host='127.0.0.1', database=self.dataBase)
            cursor = self.cnx.cursor()
            if params == None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            print("Now execute the query " + query)
            values = cursor.fetchall()
            self.cnx.commit()      # this is important, otherwise you cannot invert the record
            #print("done! nothing happened?")
            cursor.close()
            self.cnx.close()
            return values
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(err)
        except mysql.connector.Error as err:
            logging.error(err)


    def SQL_to_dataframe(self,query,params=None):
        try:
            self.cnx = mysql.connector.connect(user=self.userName, password=self.passWord, host='127.0.0.1',
                                               database=self.dataBase)
            cursor = self.cnx.cursor()
            if params == None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            print("Now execute the query " + query)
            values = cursor.fetchall()
            columnsDes=cursor.description
            columnNames=[columnsDes[i][0] for i in range(len(columnsDes))]
            self.cnx.commit()  # this is important, otherwise you cannot invert the record
            # print("done! nothing happened?")
            cursor.close()
            self.cnx.close()
            if values!=None:
                values=pd.DataFrame([list(i) for i in values],columns=columnNames)
            return values
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(err)
        except mysql.connector.Error as err:
            logging.error(err)

    def executeMultipleSQL(self, queries):
        """
        there would be queries as a list and we will execute all of the queries
        """
        try:
            self.cnx = mysql.connector.connect(user=self.userName, password=self.passWord, host='127.0.0.1', database=self.dataBase)
            cursor = self.cnx.cursor()

            for i in range(len(queries)):
                cursor.execute(queries[i])
            self.cnx.commit()
            cursor.close()
            self.cnx.close()

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(err)
        except mysql.connector.Error as err:
            logging.error(err)


    def executemanySQL(self, query, data):
        try:
            self.cnx = mysql.connector.connect(user=self.userName, password=self.passWord, host='127.0.0.1',
                                               database=self.dataBase)
            cursor = self.cnx.cursor()

            cursor.executemany(query, data)
            values = cursor.fetchall()  # fetch must before commit
            self.cnx.commit()  # this is important, otherwise you cannot invert the record
            cursor.close()
            self.cnx.close()
            return values

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(err)
        except mysql.connector.Error as err:
            logging.error(err)


    def _dropAllTables(self):
        """
        This is used for testing to drop all tables and create it again
        """
        sql_command = "DROP TABLE CoinmarketBasicTable;"
        try:
            self.executeSQL(sql_command)
        except:
            print("table already dropped?")
        #self.cnx.commit()      # this is important, otherwise you cannot invert the record

        sql_command = "DROP TABLE CoinmarketPriceTable;"
        try:
            self.executeSQL(sql_command)
        except:
            print("Table already dropped?")

        sql_command = "DROP TABLE tweets;"
        try:
            self.executeSQL(sql_command)
        except:
            print("Table already dropped?")

        sql_command = "DROP TABLE stocks;"
        try:
            self.executeSQL(sql_command)
        except:
            print("Table already dropped?")

        sql_command = "DROP TABLE Userinfo;"
        try:
            self.executeSQL(sql_command)
        except:
            print("table already dropped?")
        # self.cnx.commit()      # this is important, otherwise you cannot invert the record

        sql_command = "DROP TABLE Subscription;"
        try:
            self.executeSQL(sql_command)
        except:
            print("Table already dropped?")

        sql_command = "DROP TABLE subscription_tweets;"
        try:
            self.executeSQL(sql_command)
        except:
            print("Table already dropped?")
        #self.cnx.commit()      # this is important, otherwise you cannot invert the record

    def errorMessage(self, MESS):
        print(MESS)
        sys.exit(0)


    def _coinmarketcap_search_by_name(self, name):
        """
        Function: check if name of a crypto is in the coinmarketcap database
        Input: name
        Output:
               None: if name is not in the database
               ID: if name is found, we will return the ID of that name
        """

        query = "SELECT id from CoinmarketBasicTable where name = \""+ name + "\";"
        #print(query)
        try:
            MYcnx = mysql.connector.connect(user=self.userName, password=self.passWord, host='127.0.0.1', database=self.dataBase)
            #cursor = MYcnx.cursor(buffered=True)
            cursor = MYcnx.cursor()
            cursor.execute(query)
            #MYcnx.commit()
            result = []
            for each in cursor:
                print(each)
                result.append(each[0])      # this will get ID ?
            #print("All results are : " + str(result))
            if len(result) == 0:
                cursor.close()
                MYcnx.close()
                return None
            cursor.close()
            MYcnx.close()
            return result[0]
        except mysql.connector.Error as err:
            print(err)
            return None


    def insert_one_coinmarketcap_record(self,coinmarketcapDATA):
        """
        This function will insert one coinmarketcap data record into the database
        Example of the record is:
        {'id': 1, 'name': 'Bitcoin', 'symbol': 'BTC', 'slug': 'bitcoin', 'num_market_pairs': 9531, 'date_added': '2013-04-28T00:00:00.000Z', 'tags': ['mineable', 'pow', 'sha-256', 'store-of-value', 'state-channel', 'coinbase-ventures-portfolio', 'three-arrows-capital-portfolio', 'polychain-capital-portfolio', 'binance-labs-portfolio', 'blockchain-capital-portfolio', 'boostvc-portfolio', 'cms-holdings-portfolio', 'dcg-portfolio', 'dragonfly-capital-portfolio', 'electric-capital-portfolio', 'fabric-ventures-portfolio', 'framework-ventures-portfolio', 'galaxy-digital-portfolio', 'huobi-capital-portfolio', 'alameda-research-portfolio', 'a16z-portfolio', '1confirmation-portfolio', 'winklevoss-capital-portfolio', 'usv-portfolio', 'placeholder-ventures-portfolio', 'pantera-capital-portfolio', 'multicoin-capital-portfolio', 'paradigm-portfolio'], 'max_supply': 21000000, 'circulating_supply': 19067512, 'total_supply': 19067512, 'platform': None, 'cmc_rank': 1, 'self_reported_circulating_supply': None, 'self_reported_market_cap': None, 'last_updated': '2022-06-14T21:35:00.000Z', 'quote': {'USD': {'price': 21627.189239330353, 'volume_24h': 52034341169.604614, 'volume_change_24h': -21.7878, 'percent_change_1h': -1.71820042, 'percent_change_24h': -6.98252392, 'percent_change_7d': -30.88608604, 'percent_change_30d': -29.97211481, 'percent_change_60d': -46.50229268, 'percent_change_90d': -47.37991614, 'market_cap': 412376690347.2024, 'market_cap_dominance': 44.7328, 'fully_diluted_market_cap': 454170974025.94, 'last_updated': '2022-06-14T21:35:00.000Z'}}}
        """


        # check if the data is fine
        try:
            id = int(coinmarketcapDATA['id'])
            name = coinmarketcapDATA['name'].lower()
            symbol = coinmarketcapDATA['symbol'].lower()
            slug = coinmarketcapDATA['slug'].lower()
            num_market_pairs = int(coinmarketcapDATA['num_market_pairs'])
            dateAddedISOFormat = coinmarketcapDATA['date_added']

            #print("or here: ")

            tags =  coinmarketcapDATA['tags']
            if len(tags) > 2000:
                print("warning!!! we don't have enough space for your tags, need to update the database! ")
                print(str(coinmarketcapDATA))
                logging.error("The length of the tags is too long, check this "+str(coinmarketcapDATA))
                tags = tags[:2000]
            try:
                max_supply = float(coinmarketcapDATA['max_supply'])
            except:
                max_supply = -1

            try:
                circulating_supply = float(coinmarketcapDATA['circulating_supply'])
            except:
                circulating_supply = -1

            try:
                total_supply = float(coinmarketcapDATA['total_supply'])
            except:
                total_supply = -1

            platform = str(coinmarketcapDATA['platform'])
            cmc_rank = int(coinmarketcapDATA['cmc_rank'])
            try:
                self_reported_circulating_supply = float(coinmarketcapDATA['self_reported_circulating_supply'])
            except:
                self_reported_circulating_supply = -1

            try:
                self_reported_market_cap = float(coinmarketcapDATA['self_reported_market_cap'])
            except:
                self_reported_market_cap = -1
            last_updated_ISOFormat = coinmarketcapDATA['last_updated']

            #print("done here?")

            # here we hardcode to be the USD, not sure if we need other, may need to update here
            try:
                price = float(coinmarketcapDATA['quote']['USD']['price'])
            except:
                print("no price? skip ")
                return 0


            try:
                volume_24h = float(coinmarketcapDATA['quote']['USD']['volume_24h'])
            except:
                volume_24h = -5201314
            try:
                volume_change_24h = float(coinmarketcapDATA['quote']['USD']['volume_change_24h'])
            except:
                volume_change_24h = -5201314
            try:
                percent_change_1h = float(coinmarketcapDATA['quote']['USD']['percent_change_1h'])
            except:
                percent_change_1h = -5201314
            try:
                percent_change_24h = float(coinmarketcapDATA['quote']['USD']['percent_change_24h'])
            except:
                percent_change_24h = -5201314
            try:
                percent_change_7d = float(coinmarketcapDATA['quote']['USD']['percent_change_7d'])
            except:
                percent_change_7d = -5201314

            #print("now checking" + str(coinmarketcapDATA))
            try:
                percent_change_30d = float(coinmarketcapDATA['quote']['USD']['percent_change_30d'])
            except:
                percent_change_30d = -5201314
            try:
                percent_change_60d = float(coinmarketcapDATA['quote']['USD']['percent_change_60d'])
            except:
                percent_change_60d = -5201314
            try:
                percent_change_90d = float(coinmarketcapDATA['quote']['USD']['percent_change_90d'])
            except:
                percent_change_90d = -5201314
            try:
                market_cap = float(coinmarketcapDATA['quote']['USD']['market_cap'])
            except:
                market_cap = -5201314
            try:
                market_cap_dominance = float(coinmarketcapDATA['quote']['USD']['market_cap_dominance'])
            except:
                market_cap_dominance = -5201314
            try:
                fully_diluted_market_cap = float(coinmarketcapDATA['quote']['USD']['fully_diluted_market_cap'])
            except:
                fully_diluted_market_cap = -5201314

            last_updated_ISOFormat_price = coinmarketcapDATA['quote']['USD']['last_updated']


            #print("Now do the database part:")
            #1. the basic information of your crypto
            userIDFind = self._coinmarketcap_search_by_name(name)

            #sys.exit(0)
            #print("find userID : " + str(userIDFind))
            if userIDFind is None:
                # we don't find this crypto name, we will add it as a new one and update the price
                queries = []
                query = "SET @dateadded = CAST(\"" + dateAddedISOFormat  + "\" AS DATETIME);"
                #self.executeSQL(query)
                queries.append(query)

                query = "SET @lastUpdated = CAST(\"" + last_updated_ISOFormat  + "\" AS DATETIME);"
                #self.executeSQL(query)
                queries.append(query)

                query = "INSERT INTO CoinmarketBasicTable (id, name, symbol, slug, num_market_pairs, date_added, tags, max_supply, circulating_supply, total_supply, platform, cmc_rank, self_reported_circulating_supply, self_reported_market_cap, last_updated) VALUES "+"(\""+str(id)+"\",\""+str(name)+"\",\""+str(symbol)+"\",\""+str(slug)+"\",\""+str(num_market_pairs)+"\","+"@dateadded"+",\""+str(tags)+"\",\""+str(max_supply)+"\",\""+str(circulating_supply)+"\",\""+str(total_supply)+"\",\""+str(platform)+"\",\""+str(cmc_rank)+"\",\""+str(self_reported_circulating_supply)+"\",\""+str(self_reported_market_cap)+"\","+"@lastUpdated"+");"
                queries.append(query)
                #print(queries)
                self.executeMultipleSQL(queries)

                #2. the price information
                queries = []
                query = "SET @lastUpdated = CAST(\"" + last_updated_ISOFormat_price  + "\" AS DATETIME);"
                queries.append(query)
                query = "INSERT INTO CoinmarketPriceTable (id, price, volume_24h, volume_change_24h,  percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated) VALUES "+"(\""+str(id)+"\",\""+str(price)+"\",\""+str(volume_24h)+"\",\""+str(volume_change_24h)+"\",\""+str(percent_change_1h)+"\",\""+str(percent_change_24h)+"\",\""+str(percent_change_7d)+"\",\""+str(percent_change_30d)+"\",\""+str(percent_change_60d)+"\",\""+str(percent_change_90d)+"\",\""+str(market_cap)+"\",\""+str(market_cap_dominance)+"\",\""+str(fully_diluted_market_cap)+"\","+"@lastUpdated"+");"
                queries.append(query)
                #print(queries)
                self.executeMultipleSQL(queries)

            else:
                print("Great, we get the id: " + str(userIDFind))
                print("Now we will update the record ... ")
                queries = []
                query = "SET @dateadded = CAST(\"" + dateAddedISOFormat  + "\" AS DATETIME);"
                #self.executeSQL(query)
                queries.append(query)

                query = "SET @lastUpdated = CAST(\"" + last_updated_ISOFormat  + "\" AS DATETIME);"
                #self.executeSQL(query)
                queries.append(query)

                query = "UPDATE CoinmarketBasicTable SET " + " name = \""+str(name)+"\", symbol = \""+str(symbol)+"\", slug = \""+str(slug)+"\", num_market_pairs = \""+str(num_market_pairs)+"\", date_added = "+"@dateadded"+", tags = \""+str(tags)+"\", max_supply = \""+str(max_supply)+"\", circulating_supply = \""+str(circulating_supply)+"\", total_supply = \""+str(total_supply)+"\", platform = \""+str(platform)+"\", cmc_rank = \""+str(cmc_rank)+"\", self_reported_circulating_supply = \""+str(self_reported_circulating_supply)+"\", self_reported_market_cap = \""+str(self_reported_market_cap)+"\",last_updated = "+"@lastUpdated"+" where id =  \""+str(id)+"\""
                #query= "UPDATE CoinmarketBasicTable SET " + "symbol = \""+str(symbol)+'sdsadaa'+"\"  where id =  \""+str(id)+"\";"
                queries.append(query)
                print(queries)
                self.executeMultipleSQL(queries)

                #2. the price information
                queries = []
                query = "SET @lastUpdated = CAST(\"" + last_updated_ISOFormat_price  + "\" AS DATETIME);"
                queries.append(query)
                query = "INSERT INTO CoinmarketPriceTable (id, price, volume_24h, volume_change_24h,  percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated) VALUES "+"(\""+str(id)+"\",\""+str(price)+"\",\""+str(volume_24h)+"\",\""+str(volume_change_24h)+"\",\""+str(percent_change_1h)+"\",\""+str(percent_change_24h)+"\",\""+str(percent_change_7d)+"\",\""+str(percent_change_30d)+"\",\""+str(percent_change_60d)+"\",\""+str(percent_change_90d)+"\",\""+str(market_cap)+"\",\""+str(market_cap_dominance)+"\",\""+str(fully_diluted_market_cap)+"\","+"@lastUpdated"+");"
                queries.append(query)
                #print(queries)
                self.executeMultipleSQL(queries)

                print("Done, success!")

        except BaseException as err:
            print(err)
            logging.error("\n!!!the data format is not correct, cannot store the basic information or price , check here" + str(coinmarketcapDATA))

        # #1. the basic information of your crypto
        # if self._coinmarketcap_search_by_name(name) is None:
        #     # we don't find this crypto name, we will add it as a new one and update the price
        #     query = "SET @dateadded = CAST(" + dateAddedISOFormat  + " AS DATETIME);"
        #     query = query+"SET @lastUpdated = CAST(" + last_updated_ISOFormat  + " AS DATETIME)"
        #     query = query + ";" + "INSERT INTO CoinmarketBasicTable (id, name, symbol, slug, num_market_pairs, date_added, tags, max_supply, circulating_supply, total_supply, platform, cmc_rank, self_reported_circulating_supply, self_reported_market_cap, last_updated) VALUES "+"(\""+id+"\",\""+name+"\",\""+symbol+"\",\""+slug+"\",\""+num_market_pairs+"\",\""+"@dateadded"+"\",\""+tags+"\",\""+max_supply+"\",\""+circulating_supply+"\",\""+total_supply+"\",\""+platform+"\",\""+cmc_rank+"\",\""+self_reported_circulating_supply+"\",\""+self_reported_market_cap+"\",\""+"@lastUpdated"+"\");"
        #     print(query)





def main():
    SQL_DB(userName= "database user name", passWord= "your database password", dataBase = "your database name")
main()
