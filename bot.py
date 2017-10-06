# -*- coding: utf-8 -*-
"""
Created on Thu Oct  5 16:50:36 2017

@author: Sarai
"""

import sys
import json
import tweepy
from tweepy.streaming import StreamListener
import pymysql.cursors
import config_local as config  

def db_connect():
    # Connect to the database
    connection = pymysql.connect(host = config.host,
                                 user = config.user,
                                 password = config.password,
                                 db = config.database,
                                 charset = 'utf8mb4',
                                 cursorclass = pymysql.cursors.DictCursor)
    
    return connection    


class StdOutListener( StreamListener ):

    def __init__( self ):
        self.tweetCount = 0

    def on_connect( self ):
        print("Connection established!")

    def on_disconnect( self, notice ):
        print("Connection lost! : ", notice)

    def on_data( self, status ):
        print("Entered on_data()")
        global current_status
        current_status = status
#        print(status, flush = True)
        
        global dm
        dm = json.loads(status).get('direct_message')
        
        if dm != None:
            print("DM from " + dm['sender_screen_name'] + ": \"" + dm['text'] + "\"")
            global result
            result = insert_receipt(dm)
            sys.exit()
        
        return True

    def on_direct_message( self, status ):
        print("Entered on_direct_message()")
        try:
            print(status, flush = True)
            return True
        except BaseException as e:
            print("Failed on_direct_message()", str(e))

    def on_error( self, status ):
        print(status)


def insert_receipt(dm):
    # Insert DM contents into DB receipts table
    sender_id = dm['sender_id']
    recipient_id = dm['recipient_id']
    text = dm['text']
    
    connection = db_connect()
    
    try:
        with connection.cursor() as cursor:
            # Create a new record in receipts table
            sql = "INSERT INTO `receipts` (`twitter_id`, `blocklist_id`, `contents_text`) VALUES (%s, %s, %s)"
            cursor.execute(sql, (sender_id, recipient_id, text))
    
        # Commit to save changes
        connection.commit()
    
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id` FROM `receipts` WHERE `twitter_id`=%s AND `blocklist_id`=%s LIMIT 1"
            cursor.execute(sql, (sender_id,recipient_id,))
            result = cursor.fetchone()
                
    except BaseException as e:
        return "Error in insert_receipt()"

    finally:
        connection.close()
        return "Successfully inserted DM into receipts database, id " + str(result)


def main():

    try:
        auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
        auth.secure = True
        auth.set_access_token(config.access_token, config.access_token_secret)

        api = tweepy.API(auth)

        # If the authentication was successful, you should
        # see the name of the account print out
        print(api.me().name)

        stream = tweepy.Stream(auth, StdOutListener())

        stream.userstream()

    except BaseException as e:
        print("Error in main()", e)

if __name__ == '__main__':
    main()