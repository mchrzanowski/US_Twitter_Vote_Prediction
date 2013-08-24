import constants
import enchant
import multiprocessing
import MySQLdb
import re

from nltk.stem.lancaster import LancasterStemmer
from sklearn.feature_extraction.text import CountVectorizer

# make all this global to pickle properly
# for multiprocessing support
# ugly but effective.
tokenizer = CountVectorizer().build_tokenizer()
stemmer = LancasterStemmer()
spell_checker = None


def insert_data_into_table(cursor, data, table):
    cursor.executemany(" insert ignore into " + table + """
        (tweet_id, stemmed_text) values (%s, %s)""", data)


def process_tweet_data(data, insertion_data):
    id, raw_text = data

    global spell_checker, stemmer, tokenizer

    clean_text = sub_numbers(raw_text)
    new_words = []

    for word in tokenizer(clean_text):
        new_word = word

        if spell_checker != None and constants.NUMBER_SUBBED_TOKEN not in new_word:
            if not spell_checker.check(new_word):
                suggestions = spell_checker.suggest(new_word)
                if len(suggestions) > 0:
                    new_word = suggestions[0]

        new_word = stemmer.stem(new_word)
        new_words.append(new_word)

    stemmed_text = ' '.join(new_words)

    insertion_data.append((id, stemmed_text))


def run(stem_all=False, username=None, force_user_stem=False, spell_check=False):

    manager = multiprocessing.Manager()
    insertion_data = manager.list()


    db = MySQLdb.connect(user=constants.mysql_user,
        passwd=constants.mysql_password,
        db=constants.mysql_database,
        charset=constants.mysql_charset)
    db.autocommit(True)

    cursor = db.cursor()

    BATCH_SIZE = 100000

    if spell_check:
        print "Spell Checking: ON"
        global spell_checker
        spell_checker = enchant.Dict("en_US")
        table_to_use = constants.STEMMED_SPELL_CHECKED_TWEETS_TABLE
    else:
        print "Spell Checking: OFF"
        table_to_use = constants.STEMMED_TWEETS_TABLE

    if stem_all:
        print "Preprocessing all tweets."
        print "This takes a lot of time! Sleeping for 10s, so you can still kill this process!"
        time.sleep(10)
        cursor.execute(""" delete from %s """ % table_to_use)
        cursor.execute(""" select tweet_id, tweet_text from tweets """)
    elif username != None:
        if force_user_stem:
            cursor.execute(" delete s from " + table_to_use + """ s, tweets t, accounts a
                where s.tweet_id = t.tweet_id and t.account_id = a.account_id and
                a.account_name = %s """, username)
        print "Preprocessing tweets from %s" % username
        cursor.execute(""" select t.tweet_id, t.tweet_text from tweets t inner join
            accounts a on a.account_id = t.account_id left join """ + table_to_use + """ s
            on t.tweet_id = s.tweet_id where s.stemmed_text is NULL and
            a.account_name = %s """, username)
            
    else:
        print "Preprocessing new tweets."
        cursor.execute(""" select tweet_id, tweet_text from tweets t
            where t.tweet_id  not in (select tweet_id from %s) """ % table_to_use)

    pool = multiprocessing.Pool()
    for i, row in enumerate(cursor.fetchall()):
        pool.apply_async(process_tweet_data, args=(row, insertion_data))
        if i > 0 and i % BATCH_SIZE == 0:
            print "Processing Batch: %s" % (i // BATCH_SIZE)
            # run this pool.
            pool.close()
            pool.join()

            # insert new data.
            insert_data_into_table(cursor, insertion_data, table_to_use)
            del insertion_data[:]

            # spawn new pool.
            pool = multiprocessing.Pool()

    # take care of remaining tweets.
    print "Processing Last Batch."
    pool.close()
    pool.join()
    insert_data_into_table(cursor, insertion_data, table_to_use)


def sub_numbers(text):
    return re.sub("[0-9]+", " " + constants.NUMBER_SUBBED_TOKEN + " " , text)


if __name__ == '__main__':
    import argparse
    import time

    parser = argparse.ArgumentParser(description="Stem raw tweet data.")
    parser.add_argument('--all', action='store_true', help="""Stem all tweets.
     Otherwise does new ones only.""")
    parser.add_argument('--spell_check', action='store_true', help="""Perform
        spell checking before stemming. """)
    parser.add_argument('-user', type=str, help="Stem tweets from one user")

    args = vars(parser.parse_args())
    start = time.time()
    run(stem_all=args['all'], username=args['user'], spell_check=args['spell_check'])
    end = time.time()
    print "Runtime: %f seconds." % (end - start)
