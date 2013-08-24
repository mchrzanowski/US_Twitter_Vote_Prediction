import constants
import MySQLdb
import numpy

_db = MySQLdb.connect(user=constants.mysql_user,
    passwd=constants.mysql_password,
    db=constants.mysql_database,
    charset=constants.mysql_charset)

_cursor = _db.cursor()


def pull_tweets_from_user(username, stemmed=True):
    ''' get all tweets for a specific user from the db '''

    print "Data: Account-Specific Tweets"

    if stemmed:
        _cursor.execute(""" select s.stemmed_text, t.account_id, t.tweet_id
            from stemmed_tweets s, tweets t, accounts a
            where t.tweet_id = s.tweet_id and t.account_id = a.account_id
            and a.account_name like %s """, username)

    else:
        _cursor.execute(""" select tweet_text, account_id,
            tweet_id from tweets t, accounts a
            where t.account_id = a.account_id
            and a.account_name like %s """, username)

    return _cursor.fetchall()


def pull_account_grouped_hashtags(labels):
    ''' create set of user/ all hashtags '''

    print "Data: Account-Grouped Hashtags"

    accounts = dict()

    _cursor.execute(""" select t.account_id, h.hashtag
        from hashtags h, tweets t
        where t.tweet_id = h.tweet_id """)

    for datum in _cursor.fetchall():
        account_id, hashtag = datum
        if account_id not in labels:
            continue
        if account_id not in accounts:
            accounts[account_id] = list()
        accounts[account_id].append(hashtag)

    return tuple((' '.join(accounts[id]), id) for id in accounts)


def pull_account_grouped_tweets(labels, stemmed=True, predefined_training_size=None, use_spell_checked_text=False):

    print "Data: Account-Grouped Tweets"
    print "Using Stemmed Text?: %s" % ("YES" if stemmed else "NO")
    print "Using Stemmed, Spell-Checked Text? %s" % ("YES" if stemmed and use_spell_checked_text else "NO")

    account_data = dict()

    if stemmed:
        if use_spell_checked_text:
            table = constants.STEMMED_SPELL_CHECKED_TWEETS_TABLE
        else:
            table = constants.STEMMED_TWEETS_TABLE
        _cursor.execute(""" select s.stemmed_text, t.account_id
            from %s s, tweets t where t.tweet_id = s.tweet_id """ % table)
    else:
        _cursor.execute(""" select tweet_text, account_id,
            from tweets """)

    rows = _cursor.fetchall()

    if predefined_training_size != None:
        RATIO = float(predefined_training_size) / len(rows)
    else:
        RATIO = -1

    for i, datum in enumerate(rows):

        text, account_id = datum
        if account_id not in labels:
            continue

        if RATIO != -1:
            sampling = numpy.random.rand()
            if sampling > RATIO:
                continue

        if account_id not in account_data:
            account_data[account_id] = set()

        account_data[account_id].add(text)

    return tuple((' '.join(account_data[id]), id) for id in account_data)


def pull_tweet_hashtags(labels, predefined_training_size=None):

    print "Data: Hashtags"
    
    hashtags = dict()
    accounts = dict()

    _cursor.execute(""" select t.tweet_id, h.hashtag, t.account_id
        from hashtags h, tweets t
        where t.tweet_id = h.tweet_id """)

    for data in _cursor.fetchall():
        tweet_id, hashtag, account_id = data
        if tweet_id not in hashtags:
            hashtags[tweet_id] = set()
        hashtags[tweet_id].add(hashtag)
        if tweet_id not in accounts:
            accounts[tweet_id] = account_id

    if predefined_training_size != None:
        RATIO = float(predefined_training_size) / len(hashtags)
    else:
        RATIO = -1

    tweet_data = set()
    for tweet_id in hashtags:

        if accounts[tweet_id] not in labels:
            continue

        if RATIO != -1:
            sampling = numpy.random.rand()
            if sampling > RATIO:
                continue

        tweet_hashtags = ' '.join(hashtags[tweet_id])
        tweet_data.add((tweet_hashtags, accounts[tweet_id], tweet_id))

    return tweet_data


def pull_tweets(labels, stemmed=True, predefined_training_size=None, use_spell_checked_text=False):

    print "Data: Tweets"
    print "Using Stemmed Text?: %s" % ("YES" if stemmed else "NO")
    print "Using Stemmed, Spell-Checked Text? %s" % ("YES" if stemmed and use_spell_checked_text else "NO")

    tweet_data = set()

    if stemmed:
        if use_spell_checked_text:
            table = constants.STEMMED_SPELL_CHECKED_TWEETS_TABLE
        else:
            table = constants.STEMMED_TWEETS_TABLE
        _cursor.execute(""" select s.stemmed_text, t.account_id, t.tweet_id
            from %s s, tweets t where t.tweet_id = s.tweet_id """ % table)
    else:
        _cursor.execute(""" select tweet_text, account_id,
            tweet_id from tweets """)

    rows = _cursor.fetchall()

    if predefined_training_size != None:
        RATIO = float(predefined_training_size) / len(rows)
    else:
        RATIO = -1

    for i, data in enumerate(rows):

        account_id = data[1]
        if account_id not in labels:
            continue

        if RATIO != -1:
            sampling = numpy.random.rand()
            if sampling > RATIO:
                continue

        tweet_data.add(data)

    return tweet_data


def pull_follower_data():
    '''
        Output:
            * A dict of account_id => list of whether that account is
                following certain people
            * the length of each array that is a value in the dict
    '''
    
    _cursor.execute(""" select account_id, following_obama,
        following_biden, following_romney, following_ryan
        from candidate_following """)
    following = dict()
    for row in _cursor.fetchall():
        following[row[0]] = numpy.zeros(len(row) -1, dtype=numpy.int8)
        for i in xrange(1, len(row)):
            following[row[0]][i - 1] = row[i]

    return following, 4


def pull_anchor_classifications():
    ''' 
        Output:
            * a dict of account_id => vote
    '''
    labels = dict()
    
    _cursor.execute(""" select vote, account_id from anchors """)
    for row in _cursor.fetchall():
        if row[0].lower() == constants.OBAMA_VOTE:
            labels[row[1]] = -1
        elif row[0].lower() == constants.ROMNEY_VOTE:
            labels[row[1]] = 1

    return labels


def pull_tweet_ids_from_politicians():
    '''
        Output:
            * a set of all tweet IDs from tweets from politicians in the corpus
    '''
    _cursor.execute(""" select t.tweet_id from tweets t, anchors a,
        industries i where t.account_id = a.account_id and
        a.industry_id = i.industry_id and i.industry in ('politics') """)

    return set(row[0] for row in _cursor.fetchall())


def get_tweet_timestamps():
    '''
        Output:
            * a dict of tweet_id => UTC time tweet was created
    '''
    _cursor.execute(""" select t.tweet_id, t.created_at from tweets t """)

    mappings = dict()
    for row in _cursor.fetchall():
        tweet_id, raw_time = row
        timestamp = raw_time.timetuple()
        mappings[tweet_id] = timestamp

    return mappings
