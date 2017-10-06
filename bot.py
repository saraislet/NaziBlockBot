# -*- coding: utf-8 -*-
"""
Created on Thu Oct  5 16:50:36 2017

@author: Sarai
"""

import json, sys, re, requests
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
            print(result)
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
    contents = dm['text']
    parsed_text = parse_url_from_text(contents)
    text = parsed_text[0]
    tweet_url = parsed_text[1]
    
    # Test if the URL the DM is a Twitter Status, then pull data from API.
    if verify_twitter_url(tweet_url):
        status = get_tweet_from_url(tweet_url)
        twitter_id = status.user.id
        screen_name = status.user.screen_name
        name = status.user.name
        tweet = unshorten_urls_in_text(status.full_text)
        tweet_text = remove_ats(tweet)
    
    # Test if the sender is a blocklist admin.
    if verify_blocklist_admin(sender_id, recipient_id):
        approved_by_id = sender_id
    else:
        approved_by_id = None
    
    connection = db_connect()
    
    try:
        with connection.cursor() as cursor:
            # Create a new record in receipts table
            sql = "INSERT INTO `receipts` (`twitter_id`, `name`, `screen_name`, `blocklist_id`, `contents_text`, `url`, `source_user_id`, `approved_by_id`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (twitter_id, name, screen_name, recipient_id, tweet_text, tweet_url, sender_id, approved_by_id))
    
        # Commit to save changes
        connection.commit()
    
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id` FROM `receipts` WHERE `source_user_id`=%s AND `blocklist_id`=%s LIMIT 1"
            cursor.execute(sql, (sender_id,recipient_id,))
            result = cursor.fetchone()
            return "Successfully inserted DM into receipts database, id " + str(result['id'])
                
    except BaseException as e:
        return "Error in insert_receipt()"

    finally:
        connection.close()


def verify_blocklist_admin(twitter_id, blocklist_id):
    # Return true iff twitter_id, blocklist_id is listed in blocklist_admin table.
    
    connection = db_connect()
    
    try:    
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id` FROM `blocklist_admins` WHERE `admin_id`=%s AND `blocklist_id`=%s LIMIT 1"
            cursor.execute(sql, (twitter_id, blocklist_id,))
            result = cursor.fetchone()
            
            # If a matching record exists, return true, otherwise return false.
            if result == None:
                print("Sender is not a blocklist admin.")
                return False
            else:
                print("Sender is a blocklist admin.")
                return True
                
    except BaseException as e:
        print("Error in verify_blocklist_admin()")

    finally:
        connection.close()


def verify_twitter_url(url):
    # Verify that a URL belongs to a Twitter status
    match = re.match(r'https://twitter\.com/.+/status/\d+', url)
    
    if match != None:
        return True
    else:
        return False


def get_tweet_from_url(url):
    # Pull twitter status details from API and return dictionary.
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.secure = True
    auth.set_access_token(config.access_token, config.access_token_secret)
    api = tweepy.API(auth)
    
    status_id = re.sub(r'https://twitter\.com/.+/status/(\d+)', r'\1', url)
    
    # Extended tweet mode returns full text without truncation.
    return api.get_status(status_id, tweet_mode='extended')    


def parse_url_from_text(string):
    # Return an array with text and url from the string of a DM.
    
    # Match any URL beginning "http" at the end of string text.
    match = re.match(r'(.*)\s(http\S*)$', string)

    if match != None:
        text = match.group(1)
        url = match.group(2)
        return [text, unshorten_url(url)]
    else:
        return [string, ""]


def unshorten_url(url):
    # Return expanded URL (or same URL if not a redirect)
    return requests.head(url, allow_redirects=True).url


def unshorten_url_re(url):
    # Return expanded URL from regex match.
    # Unclear how to combine this with previous function nontrivially.
    # This works for now.
    return requests.head(url.group(), allow_redirects=True).url


def unshorten_urls_in_text(string):
    # Unshorten all URLs in a string.
    return re.sub(r'(https?://\S*)', unshorten_url_re, string)


def remove_ats(tweet):
    # Remove any leading @s (e.g., replies) from a tweet.
    # Any @ that is not at the beginning of a tweet will be left.
    return re.sub(r'^(@\S+\s)*', "", tweet)


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