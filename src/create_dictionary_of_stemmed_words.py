import MySQLdb
import time
import nltk


def run():

    db = MySQLdb.connect(user='polak', passwd='katy1012', db='ml2012',
        charset='utf8')

    cursor = db.cursor()

    words = set()

    cursor.execute(""" select stemmed_text from stemmed_tweets """)

    rows = cursor.fetchall()

    for data in rows:
        split_words = data[0].split()
        words.update(split_words)


if __name__ == '__main__':
    start = time.time()
    run()
    end = time.time()
    print "Runtime: %f seconds" % (end - start)
