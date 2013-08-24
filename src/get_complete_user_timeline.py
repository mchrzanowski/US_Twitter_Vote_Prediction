import os.path
import re
import twitter

_consumer_key = "qB2YpwzoOyaywFR4SynTZg"
_consumer_secret = "FcAn3Dm2Sqhhc3pirXey6FS81T7gh8U9l2TCQdA"
_access_token_key = "232113395-NDr9thbFUmKw4Mm7vxYV5ZdijT8lQuAxP8UjbPdP"
_access_token_secret = "2iSIA0bbZOuejJ6GKOS29GqAAfNbJpNG4rtZRsB1FM"

_user_directory = "/mnt/NAS/tmp/twitter/users/"

def get_list_of_all_accounts():
    for file in os.listdir(_user_directory):
        account = check_if_file_is_user_and_return_user(file)
        if account is not None:
            yield account

def get_file_for_user(user_file):
    return os.path.join(_user_directory, user_file)

def check_if_file_is_user_and_return_user(filename):
    if ".txt" not in filename:
        return None
    else:
        return filename.replace(".txt", "")

def convert_user_to_file(user):
    return user.lstrip('@').lower() + ".txt"


def does_user_have_file_already(user):
    return os.path.exists(get_file_for_user(convert_user_to_file(user)))


def handle_single_user(user, force_backfill=False):
    if force_backfill or not does_user_have_file_already(user):
        backfill(user)
    else:
        update(user)


def get_max_id_of_corpus_for_user(user):
    current_file = open(get_file_for_user(convert_user_to_file(user)))

    id_pattern = re.compile("((?<=\"id\":)\s*[0-9]+)")

    max_id = -1

    for tweet in current_file:
        matches = re.search(id_pattern, tweet)
        new_id = int(matches.group(0))
        if new_id > max_id:
            max_id = new_id

    return max_id


def update_all_users(force_backfill=False):
    for user in get_list_of_all_accounts():
        if force_backfill:
            backfill(user)
        else:
            update(user)

def update(user):
    print "Updating: %s" % user
    _api = twitter.Api(consumer_key=_consumer_key, consumer_secret=_consumer_secret,
        access_token_key=_access_token_key, access_token_secret=_access_token_secret)
    since_id = get_max_id_of_corpus_for_user(user)

    file_to_write = open(os.path.join(_user_directory,
        convert_user_to_file(user)), 'ab')

    arg_dict = {'id': user, 'count': 200, 'since_id': since_id, 'include_entities': True,
        'include_rts': True}

    while True:

        minimum_id = None
        most_recent_tweets = _api.GetUserTimeline(**arg_dict)

        if len(most_recent_tweets) == 0:
            break

        for tweet in most_recent_tweets:
            tweet_id = tweet.GetId()
            if minimum_id == None or tweet_id < minimum_id:
                minimum_id = tweet_id
            file_to_write.write(str(tweet) + '\n')

        # it might be the case that there are > 200 new tweets
        # in this case, we'll need to paginate to get them.

        # pass max_id - 1 to backfill. subtract 1 as max_id is <=, not <
        arg_dict['max_id'] = minimum_id - 1

    file_to_write.close()


def backfill(user):
    print "Backfilling: %s" % user
    _api = twitter.Api(consumer_key=_consumer_key, consumer_secret=_consumer_secret,
        access_token_key=_access_token_key, access_token_secret=_access_token_secret)
    last_tweet_id = 0

    file_to_write = open(os.path.join(_user_directory,
        convert_user_to_file(user)), 'wb')

    while True:

        arg_dict = {'id': user, 'count': 200, 'include_entities': True, 'include_rts': True}

        if last_tweet_id != 0:
            arg_dict['max_id'] = last_tweet_id - 1  # avoid duplicates

        current_page = _api.GetUserTimeline(**arg_dict)

        # have we received all the tweets we can?
        if len(current_page) == 0:
            break

        for tweet in current_page:
            file_to_write.write(str(tweet) + '\n')

        last_tweet_id = current_page[-1].GetId()

    file_to_write.close()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Get the complete timeline of a user as currently " + \
        "exists in Twitter")
    parser.add_argument('-user', help='Give Twitter account name. Pass in with an @ sign or not.')
    parser.add_argument('--all', action='store_true', help='Run for all users in corpus.')
    parser.add_argument('--force', action='store_true', help='Force a backfill.')

    args = vars(parser.parse_args())
    if args['all']:
        update_all_users(force_backfill=args['force'])
    elif args['user'] is not None:
        handle_single_user(args['user'], force_backfill=args['force'])
