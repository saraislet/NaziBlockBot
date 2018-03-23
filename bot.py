# -*- coding: utf-8 -*-
"""
Created on Thu Oct  5 16:50:36 2017

@author: Sarai
"""

import random
import json, os, re, requests, datetime
import tweepy
from tweepy.streaming import StreamListener
import pymysql.cursors
#import config_local as config

consumer_key = os.environ['consumer_key']
consumer_secret = os.environ['consumer_secret']
access_token = os.environ['access_token']
access_token_secret = os.environ['access_token_secret']
blocklist_id = os.environ['blocklist_id']

host = "http://www.receiptacle.org"

TYs = ["Thank you. Receipts database updated: ",
       "Thank you! Added this receipt: ",
       "Thanks! Updated receipts database: ",
       "TY. Receiptacle updated: ",
       "Added to Receiptacle, TY: "]

NO_TWITTER_URL = ["Tweet does not contain a Twitter status URL.",
                  "Tweet does not contain link to a Twitter status.",
                  "Tweet doesn't include a Twitter status URL."]

NO_TWITTER_URL = ["Please send me a tweet directly.\
                    Your message must contain a tweet URL.",
                   "Please DM a tweet to me directly.\
                    Your message must include a tweet URL.",
                   "Please DM a tweet to me.\
                    Include a tweet URL in your message."]

ALREADY_REPORTED = ["You've already reported this receipt: ",
                    "You already sent this to me: ",
                    "This is in Receiptacle: "]

ALREADY_RECEIPT = ["Thank you. This receipt is in the database: ",
                   "Thanks! This receipt is in Receiptacle: ",
                   "TY. Receipt on Receiptacle: "]

def db_connect():
    # Connect to the database
    connection = pymysql.connect(host = os.environ['host'],
                                 user = os.environ['user'],
                                 password = os.environ['password'],
                                 db = os.environ['database'],
                                 charset = 'utf8mb4',
                                 cursorclass = pymysql.cursors.DictCursor)
    
    return connection    


class StdOutListener( StreamListener ):

    def __init__( self ):
        pass

    def on_connect( self ):
        print("Connection established!")

    def on_disconnect( self, notice ):
        print("Connection lost! : ", notice)

    def on_data( self, status ):
        print("Entered on_data()")
        global current_status
        current_status = status
        
        global dm
        dm = json.loads(status).get('direct_message')
        
        if dm != None and str(dm['sender_id']) != str(blocklist_id):
            output = "DM from " + dm['sender_screen_name'] + ": \""
            output += unshorten_urls_in_text(dm['text']) + "\""
            print(output)
            insert_receipt(dm)
        
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
    # TODO: Improve error handling here.
    
    # Insert DM contents into DB receipts table
    sender_id = dm['sender_id']
    recipient_id = dm['recipient_id']
    contents = dm['text']
    parsed_text = parse_end_url_from_text(contents)
    tweet_url = parsed_text[1]
    
    # Get API to send to methods below
    api = get_api()
    connection = db_connect()

    # Test if the sender is a blocklist admin.
    if verify_blocklist_admin(sender_id, recipient_id, connection):
        approved_by_id = sender_id
        
    else:
        approved_by_id = None
        
    # Test if DM contains a Twitter Status, then pull data from API.
    if verify_twitter_status_url(tweet_url) == False:
        #Handle tweets without URLs
        output = "Tweet does not contain a Twitter status URL."
        output += " URL is \"" + tweet_url + "\" "
        print(output)
        
        message = random.choice(NO_TWITTER_URL)
        api.send_direct_message(sender_id, text=message)
        return
    
    else:
        status = get_tweet_from_url(tweet_url, api)
        status_id = status.id
        twitter_id = status.user.id
        screen_name = status.user.screen_name
        name = status.user.name
        tweet = unshorten_urls_in_text(status.full_text)
        tweet_text = remove_ats(tweet)
        date_of_tweet = status.created_at
        date_added = datetime.datetime.now()
        message = ""
        
        # Add or update the twitter account in the accounts table.
        check_account(twitter_id, connection, api)
        
        try:
            with connection.cursor() as cursor:
                # Check for existing receipt log.
                print("Checking for receipt log.")
                sql = "SELECT `id` FROM `receipt_logs`"
                sql += " WHERE `status_id`=%s AND `source_user_id`=%s"
                sql += " ORDER BY `id` DESC LIMIT 1"
                cursor.execute(sql, (status_id, sender_id))
                result = cursor.fetchone()
            
                if result == None:
                    # Insert receipt log
                    print("Inserting receipt log.")
                    sql = "INSERT INTO `receipt_logs`"
                    sql += " (`baddie_id`, `source_user_id`, `blocklist_id`,"
                    sql += " `status_id`, `date_added`, `text`)"
                    sql += " VALUES (%s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (twitter_id, sender_id, recipient_id, 
                                         status_id, date_added, contents))
                    connection.commit()
                    
                    # Check that receipt log was inserted.
                    sql = "SELECT `id` FROM `receipt_logs`"
                    sql += " WHERE `status_id`=%s AND `source_user_id`=%s"
                    sql += " ORDER BY `id` DESC LIMIT 1"
                    cursor.execute(sql, (status_id, sender_id))
                    result = cursor.fetchone()
                    print("Successfully inserted DM into receipt_logs, id " + str(result['id']) )
                    
                else:
                    print("Sender has already reported this tweet.")
                    message = random.choice(ALREADY_REPORTED)
                    message += host + "/search/" + screen_name + "?show_all=True"
                    api.send_direct_message(sender_id, text=message)
                    return
                
                # Check for existing receipt.
                print("Checking for receipt.")
                sql = "SELECT `id` FROM `receipts`"
                sql += " WHERE `status_id`=%s"
                sql += " ORDER BY `id` DESC LIMIT 1"
                cursor.execute(sql, (status_id,))
                result = cursor.fetchone()
                
                if result == None:
                    # Insert receipt
                    print("Inserting receipt.")
                    sql = "INSERT INTO `receipts`"
                    sql += " (twitter_id, name, screen_name,"
                    sql += " contents_text, status_id, "
                    sql += " approved_by_id, date_of_tweet, date_added)"
                    sql += " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (twitter_id, name, screen_name, 
                                         tweet_text, status_id, approved_by_id, 
                                         date_of_tweet, date_added))
                
                    # Commit to save changes
                    connection.commit()
        
                    # Read a single record
                    sql = "SELECT `id` FROM `receipts`"
                    sql += " WHERE `status_id`=%s"
                    sql += " ORDER BY `id` DESC LIMIT 1"
                    cursor.execute(sql, (status_id,))
                    result = cursor.fetchone()
                    print("Successfully inserted DM into receipts, id " + str(result['id']) )
                
                else:
                    print("This receipt was already in Receiptacle.")
                    message = random.choice(ALREADY_RECEIPT)
                    message += host + "/search/" + screen_name + "?show_all=True"
            
            # Create the block.
            # Note that block creation _must_ come after successful insertion!
            if approved_by_id is not None:
                api.create_block(twitter_id)
                print("Successfully blocked @" + screen_name)
            else:
                output = "approved_by_id is \"" + str(approved_by_id)
                output += "\"; receipt must be approved manually."
                print(output)
                
            if message == "":
                message = random.choice(TYs)
                message += host + "/search/" + screen_name + "?show_all=True"
            api.send_direct_message(sender_id, text=message)
                
        except BaseException as e:
            print("Error in insert_receipt()", e)
            return

        finally:
            connection.close()
            return


def check_account(twitter_id, connection, api):
    # Test if account is in the database ,then insert or update if necessary.    
    try:    
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `twitter_id` FROM `accounts`"
            sql += " WHERE `twitter_id`=%s LIMIT 1"
            cursor.execute(sql, (twitter_id,))
            result = cursor.fetchone()
            
            # If a matching record exists, return true, otherwise return false.
            if result == None:
                print("Account is not in the database. Inserting new row.")
                insert_account(twitter_id, connection, api)
            else:
                print("Account is in the database.")
                #update_account(twitter_id, connection, api)
            return
                
    except BaseException as e:
        print("Error in check_account()", e)
        return


def insert_account(twitter_id, connection, api):
    # Pull account details from API and insert into accounts table.
    userdata = api.get_user(twitter_id)
    name = userdata.name
    screen_name = userdata.screen_name
    description = userdata.description
    url = unshorten_url(userdata.url)
    
    try:
        with connection.cursor() as cursor:
            # Create a new record in accounts table
            sql = "INSERT INTO `accounts`"
            sql += " (`twitter_id`, `name`, `screen_name`, `description`,"
            sql += " `url`, `date_updated`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (twitter_id, name, screen_name, 
                                 description, url, datetime.datetime.now(),))
        
            # Commit to save changes
            connection.commit()
    
            print("Successfully inserted @" + screen_name + " into accounts.")
            return

    except BaseException as e:
        print("Error in insert_account()", e)
        return

def update_account(twitter_id, connection, api):
    # Test if account should be updated, and update if necessary.
    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `date_updated` FROM `accounts`"
            sql += " WHERE `twitter_id`=%s LIMIT 1"
            cursor.execute(sql, (twitter_id,))
            result = cursor.fetchone()
            date_updated = result['date_updated']
            now = datetime.datetime.now()
            
            # If difference between now() and date_updated is more than 1 day, update
            delta = now.timestamp() - date_updated.timestamp()
            if delta/60/60/24 > 1:
                print("Account is out of date. Updating account.")
                userdata = api.get_user(twitter_id)
                name = userdata.name
                screen_name = userdata.screen_name
                description = userdata.description
                url = unshorten_url(userdata.url)
                
                with connection.cursor() as cursor:
                    # Update a record in accounts table
                    sql = "UPDATE `accounts` WHERE `twitter_id`=%s LIMIT 1"
                    sql += " SET `name`=%s, `screen_name`=%s,"
                    sql += " `description`=%s, `url`=%s, `date_updated`=%s"
                    cursor.execute(sql, (twitter_id, name, screen_name, 
                                         description, url, 
                                         now,))
                    print("Successfully updated @" + screen_name + " from accounts.")
        
                # Commit to save changes
                connection.commit()
            else:
                print("Account is up to date.")
        return
    
    except BaseException as e:
        print("Error in update_account()", e)
        return

def verify_blocklist_admin(twitter_id, blocklist_id, connection):
    # Return true iff twitter_id, blocklist_id is listed in blocklist_admin table.    
    try:    
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id` FROM `blocklist_admins`"
            sql += " WHERE `admin_id`=%s AND `blocklist_id`=%s LIMIT 1"
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
        print("Error in verify_blocklist_admin()", e)


def verify_twitter_status_url(url):
    # Verify that a URL belongs to a Twitter status
    match = re.match(r'https://twitter\.com/[a-zA-Z0-9_]{1,15}/status/\d+', url)
    
    if match != None:
        return True
    else:
        return False


def get_tweet_from_url(url, api):
    # Match ID from Twitter status URL and return status object.   
    status_id = re.sub(r'https://twitter\.com/[a-zA-Z0-9_]{1,15}/status/(\d+).*', r'\1', url)
    
    # Extended tweet mode returns full text without truncation.
    return api.get_status(status_id, tweet_mode='extended')    


def parse_end_url_from_text(string):
    # Return an array with text and url from the string of a DM.
    
    # Match any URL beginning "http" at the end of string text.
    match = re.match(r'(.?)\s?(http\S*)$', string)

    if match != None:
        text = match.group(1)
        url = match.group(2)
        return [text, unshorten_url(url)]
    else:
        return [string, ""]


def unshorten_url(url):
    # Return expanded URL (or same URL if not a redirect)
    if url == None:
        return None
    else:
        return requests.head(url, allow_redirects=True).url


def unshorten_url_re(url):
    # Return expanded URL from regex matches.
    # Unclear how to combine this with previous function nontrivially.
    # This works for now.
    if url == None:
        return None
    else:
        return requests.head(url.group(), allow_redirects=True).url


def unshorten_urls_in_text(string):
    # Unshorten all URLs in a string.
    return re.sub(r'(https?://\S*)', unshorten_url_re, string)


def remove_ats(tweet):
    # Remove any leading @s (e.g., replies) from a tweet.
    # Any @ that is not at the beginning of a tweet will be left.
    return re.sub(r'^(@\S+\s)*', "", tweet)


def get_api():
    # Return tweepy oauth api
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)


def main():

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    # If the authentication was successful, you should
    # see the name of the account print out
    print(api.me().name)

    stream = tweepy.Stream(auth, StdOutListener())

    stream.userstream()

if __name__ == '__main__':
    main()
