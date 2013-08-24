import constants
import numpy.random
import pull_from_db

def train_on_politician_tweets(tweet_data):
    training_rows = set()
    test_rows = set()
    politician_tweets = pull_from_db.pull_tweet_ids_from_politicians()

    for i, tweet in enumerate(tweet_data):
        tweet_id = tweet[2]
        if tweet_id in politician_tweets:
            training_rows.add(i)
        else:
            test_rows.add(i)

    return training_rows, test_rows


def random_account_partition(tweet_data, verbose=False):
    ''' split corpus into training/test sets by account '''
    if verbose:
        print "Randomly partitioning accounts using ratio: %s" % \
            constants.ACCOUNT_TRAINING_TEST_SPLIT

    account_to_indices = dict()
    for i, tweet in enumerate(tweet_data):
        account_id = tweet[1]
        if account_id not in account_to_indices:
            account_to_indices[account_id] = set()
        account_to_indices[account_id].add(i)

    training_rows = set()
    test_rows = set()
    for account in account_to_indices:
        value = numpy.random.rand()
        if value < constants.ACCOUNT_TRAINING_TEST_SPLIT:
            training_rows.update(account_to_indices[account])
        else:
            test_rows.update(account_to_indices[account])


    if verbose:
        print "Done partitioning.\nTraining Samples: %s\nTesting Samples: %s" % \
            (len(training_rows), len(test_rows))

    return training_rows, test_rows


def random_tweet_partition(tweet_data, verbose=False):
    ''' split corpus into training/test sets by tweet '''
    if verbose:
        print "Randomly partitioning tweet data using ratio: %s" % \
            constants.TWEET_TRAINING_TEST_SPLIT

    training_rows = set()
    test_rows = set()

    for i, tweet in enumerate(tweet_data):
        value = numpy.random.rand()
        if value < constants.TWEET_TRAINING_TEST_SPLIT:
            training_rows.add(i)
        else:
            test_rows.add(i)

    if verbose: 
        print "Done partitioning.\nTraining Samples: %s\nTesting Samples: %s" % \
            (len(training_rows), len(test_rows))

    return training_rows, test_rows


def temporal_partition(tweet_data, timestamp_pivot, predefined_training_size=-1, verbose=False, predefined_account_number=-1):

    from random import sample

    # show stopper. there have to be at least as many samples requested as accounts.
    if predefined_training_size  > -1 and predefined_account_number > -1 and \
    predefined_account_number > predefined_training_size:
        raise Exception("I can't guarantee %s accounts and %s samples in the training set at the same time!" % 
            (predefined_account_number, predefined_training_size))

    if verbose: print "Temporally partitioning on point: %s" % timestamp_pivot

    training_rows = set()
    test_rows = set()
    account_to_indices = dict()

    timestamps = pull_from_db.get_tweet_timestamps()

    for i, tweet in enumerate(tweet_data):
        tweet_id = tweet[2]
        if timestamps[tweet_id] < timestamp_pivot:
            training_rows.add(i)
            if predefined_account_number != -1:
                account_id = tweet[1]
                if account_id not in account_to_indices: 
                    account_to_indices[account_id] = list()
                account_to_indices[account_id].append(i)
        else:
            test_rows.add(i)

    # here's the idea here:
    # first, give priority to satisfying the training account quota over the total training sample quota
    # then, create a list of sample indices such that there is at least one tweet from
    # predefined_account_number accounts. Then, if there's room left over
    # (ie, we haven't exceeded the training sample quota yet), sample uniformly from all
    # tweets in these accounts to pick up the remaining number needed.
    if predefined_account_number != -1:
        if predefined_account_number > len(account_to_indices):
            print "Not enough accounts present (%s) to guarantee inclusion of %s accounts in training set" % \
                (len(account_to_indices), predefined_account_number)
        else:
            chosen_accounts = sorted(account_to_indices.keys(), key=lambda x: len(account_to_indices[x]),
                reverse=True)[:predefined_account_number]
            training_rows = set()
            remaining_rows = set()

            for a in chosen_accounts:
                selector = numpy.random.randint(0, len(account_to_indices[a]))
                selected = account_to_indices[a].pop(selector)
                training_rows.add(selected)
                remaining_rows.update(account_to_indices[a])

            if predefined_training_size == -1:
                training_rows.update(remaining_rows)
            else:
                if len(training_rows) < predefined_training_size and \
                len(training_rows) + len(remaining_rows) > predefined_training_size and \
                predefined_training_size - predefined_account_number > 0:
                    returnable_rows = sample(remaining_rows, predefined_training_size - predefined_account_number)
                    training_rows.update(returnable_rows)
                else:
                    if verbose and len(training_rows) + len(remaining_rows) < predefined_training_size:
                        print "Not enough data (%s) to construct training set of size %s. " % \
                            (len(training_rows) + len(remaining_rows), predefined_training_size)
                    if len(training_rows) < predefined_training_size:
                        training_rows.update(remaining_rows)
                predefined_training_size = -1

    if predefined_training_size != -1:
        if predefined_training_size > len(training_rows):
            if verbose:
                print "Not enough data (%s) to construct training set of size %s. " % \
                    (len(training_rows), predefined_training_size)

        elif predefined_training_size < len(training_rows):
            
            if verbose: print "Shrinking training set to %s samples." % predefined_training_size
            # since we're splitting temporally, throw away the training
            # data we don't want.
            training_rows = set(sample(training_rows, predefined_training_size))

    if verbose: 
        print "Done partitioning.\nTraining Samples: %s\nTesting Samples: %s" % \
            (len(training_rows), len(test_rows))

    return training_rows, test_rows
