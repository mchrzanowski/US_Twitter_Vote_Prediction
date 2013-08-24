import constants
import MySQLdb
import re
import time

from nltk.stem.lancaster import LancasterStemmer
from sklearn.feature_extraction.text import CountVectorizer


def insert_data_into_stemmed_tweets_table(cursor, data):
    cursor.executemany(""" insert ignore into stemmed_tweets
        (tweet_id, stemmed_text) values (%s, %s) """,
        data)


def run(stem_all=False, username=None, force_user_stem=False, spell_check=False):

    db = MySQLdb.connect(user=constants.mysql_user,
            passwd=constants.mysql_password,
            db=constants.mysql_database,
            charset=constants.mysql_charset)
    db.autocommit(True)

    cursor = db.cursor()

    insertion_data = set()

    BATCH_SIZE = 100000

    tokenizer = CountVectorizer().build_tokenizer()
    stemmer = LancasterStemmer()

    if spell_check:
        import enchant
        spell_checker = enchant.Dict("en_US")
    else:
        spell_checker = None

    if stem_all:
        print "Stemming all tweets."
        print "This takes a lot of time! Sleeping for 10s, so you can still kill this process!"
        time.sleep(10)
        cursor.execute(""" delete from stemmed_tweets """)
        cursor.execute(""" select tweet_id, tweet_text from tweets """)
    elif username != None:
        if force_user_stem:
            cursor.execute(""" delete s from stemmed_tweets s, tweets t, accounts a
                where s.tweet_id = t.tweet_id and t.account_id = a.account_id and
                a.account_name = %s """, username)
        print "Stemming tweets from %s" % username
        cursor.execute(""" select t.tweet_id, t.tweet_text from tweets t inner join
            accounts a on a.account_id = t.account_id left join stemmed_tweets s
            on t.tweet_id = s.tweet_id where s.stemmed_text is NULL and
            a.account_name = %s """, username)
            
    else:
        print "Stemming new tweets."
        cursor.execute(""" select tweet_id, tweet_text from tweets t
            where t.tweet_id  not in (select tweet_id from stemmed_tweets) """)

    for row in cursor.fetchall():
        id, raw_text = row

        clean_text = sub_numbers(raw_text)

        new_words = []
        # stem it.
        for word in tokenizer(clean_text):

            if constants.NUMBER_SUBBED_TOKEN not in word and spell_checker != None:
                if not spell_checker.check(word):
                    suggestions = spell_checker.suggest(word)
                    if len(suggestions) > 0:
                        word = suggestions[0]

            new_word = stemmer.stem(word)
            new_words.append(new_word)

        stemmed_text = ' '.join(new_words)

        insertion_data.add((id, stemmed_text))

        if len(insertion_data) > BATCH_SIZE:
            insert_data_into_stemmed_tweets_table(cursor, insertion_data)
            insertion_data.clear()

    if len(insertion_data) > 0:
        insert_data_into_stemmed_tweets_table(cursor, insertion_data)
        insertion_data.clear()


def sub_numbers(text):
    return re.sub("[0-9]+", " " + constants.NUMBER_SUBBED_TOKEN + " " , text)


if __name__ == '__main__':
    import argparse

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
