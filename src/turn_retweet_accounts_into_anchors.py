import constants
import csv
import time
import twython

import codecs
import cStringIO


def run(tweet_id, filepath, label):

    # use Twython here as opposed to the usual twitter client
    # as the twitter module doesn't implement the most up-to-date
    # APIs of Twitter and actually can't attach a count number to
    # its GetRetweets function.
    api = twython.Twython(app_key=constants.consumer_key,
        app_secret=constants.consumer_secret,
        oauth_token=constants.access_token_key,
        oauth_token_secret=constants.access_token_secret)

    retweets = api.getRetweets(id=tweet_id, count=100)

    if len(retweets) == 0:
        return

    already_added_accounts = set()

    # the file is Unicode, which csv doesn't suppport natively,
    # but twitter account handles are always ascii.
    reader = csv.DictReader(open(filepath, 'rb'), delimiter=',')
    for row in reader:
        account_name = row['Twitter Account']
        already_added_accounts.add(account_name)

    # write to unicode. class provided at the csv documentation page.
    writer = UnicodeWriter(open(filepath, 'ab'), delimiter=',')

    for retweet in retweets:

        if retweet['user']['screen_name'] in already_added_accounts:
            continue
        # these classes are really primitive and only write a list
        # so pass the data as a list while trying to be generic
        # about the order of fields.
        row_to_write = ["" for _ in xrange(len(reader.fieldnames))]
        for i, field in enumerate(reader.fieldnames):
            if field == 'Name of Entity':
                row_to_write[i] = retweet['user']['name'] or ""
            elif field == 'Twitter Account':
                row_to_write[i] = retweet['user']['screen_name']
            elif field == 'Website Affiliated with Entity':
                row_to_write[i] = retweet['user']['url'] or ""
            elif field == 'Endorses which candidate':
                row_to_write[i] = label
            elif field == 'If vote is not obvious, citation':
                row_to_write[i] = str(tweet_id)
            elif field == 'Industry':
                row_to_write[i] = 'Followed Retweet'

        writer.writerow(row_to_write)


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="""
        Get as many accounts that have retweeted a certain
        post and push it into the anchors table """)
    parser.add_argument('-id', type=int, required=True,
        help='Tweet ID to find retweeters for.')
    parser.add_argument('-file', type=str, required=True,
        help='Anchor file to append to.')
    parser.add_argument('-label', type=str, required=True,
        help='Label assignment to all harvested accounts')

    args = vars(parser.parse_args())

    start = time.time()
    run(args['id'], args['file'], args['label'])
    end = time.time()
    print "Runtime: %f seconds" % (end - start)
