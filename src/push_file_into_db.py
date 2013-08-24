import codecs
import constants
import json
import MySQLdb
import os.path
import re
import time


def unicode_encode(s):
    if s == None:
        return ""
    else:
        return s.encode('utf-8')


def produce_data_for_tweet_table(tweet):

    tweet_id = tweet['id']
    account_id = tweet['user']['id']
    tweet_text = tweet['text'] or ""
    created_at = tweet['created_at']

    # was only added in 2010.
    if 'retweet_count' in tweet:
        retweet_count = tweet['retweet_count']
    else:
        retweet_count = 0

    if 'retweeted_status' in tweet:
        original_id = tweet['retweeted_status']['id']
    else:
        original_id = None

    source = tweet['source']
    #tweet_text = unicode_encode(tweet_text)
    #source = unicode_encode(source)

    utc_time = time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
    created_at = time.strftime("%Y-%m-%d %H:%M:%S", utc_time)

    return (tweet_id, account_id, tweet_text, created_at,
        retweet_count, source, original_id)


def produce_data_for_hashtag_table(tweet):

    tweet_id = tweet['id']

    returnable = list()

    if 'entities' in tweet:
        for tag_representation in tweet['entities']['hashtags']:
            #tag = unicode_encode(tag_representation['text'])
            tag = tag_representation['text']
            if len(tag) == 0:
                continue
        returnable.append((tweet_id, tag))

    elif 'hashtags' in tweet:
        for tag in tweet['hashtags']:
            #tag = unicode_encode(tag)
            if len(tag) == 0:
                continue
        returnable.append((tweet_id, tag))

    return returnable


def produce_data_for_account_table(tweet):

    # raw account data
    account_id = tweet['user']['id']
    account_name = tweet['user']['screen_name']
    name = tweet['user']['name']

    if 'location' in tweet['user']:
        location = tweet['user']['location']
    else:
        location = ""

    if 'followers_count' in tweet['user']:
        follower_count = tweet['user']['followers_count']
    else:
        follower_count = -1

    if 'friends_count' in tweet['user']:
        friend_count = tweet['user']['friends_count']
    else:
        friend_count = -1

    if 'time_zone' in tweet['user']:
        time_zone = tweet['user']['time_zone']
    else:
        time_zone = ""

    # convert to utf-8
    #account_name = unicode_encode(account_name)
    #name = unicode_encode(name)
    #location = unicode_encode(location)
    #time_zone = unicode_encode(time_zone)

    return (account_id, account_name, name, location,
            follower_count, friend_count, time_zone)


def execute_hashtag_table_insertion(cursor, data):
    cursor.executemany(""" insert ignore into hashtags
        (tweet_id, hashtag) values (%s, %s) """, data)


def execute_tweets_table_insertion(cursor, data):
    cursor.executemany(""" insert ignore into tweets
        (tweet_id, account_id, tweet_text, created_at,
        retweet_count, source, original_tweet_id) values
        (%s, %s, %s, %s, %s, %s, %s) """, data)


def execute_accounts_table_insertion(cursor, data):
    cursor.executemany(""" insert ignore into accounts
        (account_id, account_name, name, location, followers,
        friends, time_zone) values (%s, %s, %s, %s, %s, %s, %s)
        """, data)


def run(path):
    db = MySQLdb.connect(user=constants.mysql_user, passwd=constants.mysql_password,
            db=constants.mysql_database, charset=constants.mysql_charset)
    db.autocommit(True)
    cursor = db.cursor()

    if os.path.isdir(path):
        for file in os.listdir(path):
            parse_file(os.path.join(path, file), cursor)
    else:
        parse_file(path, cursor)

    cursor.close()


def parse_file(file, cursor):

    print "Parsing %s" % file

    BATCH_TRANSACTION_SIZE = 400000

    account_data = set()
    tweet_data = set()
    hashtag_data = set()

    int_row_check = re.compile("^\s*[0-9]+\s*$")

    with codecs.open(file, mode='rb', encoding='utf-8') as fh:
        for row in fh:

            # check if this row's just an integer.
            if re.match(int_row_check, row):
                continue

            try:
                tweet = json.loads(row)
            except Exception:
                print "Bad row: %s" % row
                continue

            # deletion tweet. ignore it.
            if 'delete' in tweet:
                continue

            # skip if not english
            if tweet['user']['lang'] != 'en':
                continue

            # ignore everything after Nov 6th.
            tweet_utc_time = time.strptime(tweet['created_at'], 
                '%a %b %d %H:%M:%S +0000 %Y')
            if min(constants.election_utc_time, tweet_utc_time) == constants.election_utc_time:
                continue

            # parse tweet for account data.
            account_data.add(produce_data_for_account_table(tweet))

            # do the same for the actual tweet data (excluding stuff in entities).
            tweet_data.add(produce_data_for_tweet_table(tweet))

            # hashtag data.
            hashtag_data.update(produce_data_for_hashtag_table(tweet))

            if len(account_data) > BATCH_TRANSACTION_SIZE:
                execute_accounts_table_insertion(cursor, account_data)
                execute_tweets_table_insertion(cursor, tweet_data)
                execute_hashtag_table_insertion(cursor, hashtag_data)

                account_data.clear()
                tweet_data.clear()
                hashtag_data.clear()

    if len(account_data) > 0:
        execute_accounts_table_insertion(cursor, account_data)
        account_data.clear()

    if len(tweet_data) > 0:
        execute_tweets_table_insertion(cursor, tweet_data)
        tweet_data.clear()

    if len(hashtag_data) > 0:
        execute_hashtag_table_insertion(cursor, hashtag_data)
        hashtag_data.clear()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Push raw twitter data into database")
    
    parser.add_argument('-path', type=str, required=True,
        help='Provide a path to file(s) to push.')

    args = vars(parser.parse_args())

    start = time.time()
    run(args['path'])
    end = time.time()
    print "Runtime: %f seconds" % (end - start)
