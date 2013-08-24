import codecs
import constants
import json
import os.path
import re
import time
import twitter

def get_list_of_all_accounts():
    for file in os.listdir(constants.user_directory):
        account = check_if_file_is_user_and_return_user(file)
        if account is not None:
            yield account

def get_file_for_user(user_file):
    return os.path.join(constants.user_directory, user_file)

def check_if_file_is_user_and_return_user(filename):
    if ".txt" not in filename:
        return None
    else:
        return filename.replace(".txt", "")

def convert_user_to_file(user):
    return user.lstrip('@').lower() + ".txt"


def does_user_have_file_already(user):
    return os.path.exists(get_file_for_user(convert_user_to_file(user)))


def handle_single_user(user, force_backfill=False, new_only=False):
    if force_backfill or not does_user_have_file_already(user):
        backfill(user)
    elif not new_only:
        update(user)


def get_max_id_and_time_of_corpus_for_user(user):
    current_file = codecs.open(get_file_for_user(convert_user_to_file(user)),
        mode='rb', encoding='utf-8')

    max_id = -1
    max_time = -1

    for row in current_file:
        tweet = json.loads(row)
        new_id = tweet['id']
        time_stamp = tweet['created_at']

        if new_id > max_id:
            max_id = new_id
            max_time = time_stamp

    if max_time != -1:
        utc_time = time.strptime(max_time, '%a %b %d %H:%M:%S +0000 %Y')
    else:
        utc_time = None

    return max_id, utc_time

def process_file(file_path, force_backfill=False, new_only=False):
    with codecs.open(file_path, mode='rb', encoding='utf-8') as fh:
        for row in fh:
            user = row[1].lstrip('@').lower()
            if force_backfill or not does_user_have_file_already(user):
                backfill(user)
            elif not new_only:
                update(user)


def update_all_users(force_backfill=False, new_only=False):
    for user in get_list_of_all_accounts():
        if force_backfill:
            backfill(user)
        elif not new_only:
            update(user)

def update(user):
    print "Updating: %s" % user
    _api = twitter.Api(consumer_key=constants.consumer_key, consumer_secret=constants.consumer_secret,
        access_token_key=constants.access_token_key, access_token_secret=constants.access_token_secret)
    since_id, since_time = get_max_id_and_time_of_corpus_for_user(user)

    if since_time == None:
        return

    if min(constants.election_utc_time, since_time) == constants.election_utc_time:
        return

    file_to_write = codecs.open(os.path.join(constants.user_directory,
        convert_user_to_file(user)), mode='ab', encoding='utf-8')

    arg_dict = {'id': user, 'count': 200, 'since_id': since_id, 'include_entities': True,
        'include_rts': True}

    while True:

        minimum_id = None
        
        most_recent_tweets = perform_api_user_timeline_call(_api, arg_dict)

        if len(most_recent_tweets) == 0:
            break

        for tweet in most_recent_tweets:
            tweet_id = tweet.GetId()
            if minimum_id == None or tweet_id < minimum_id:
                minimum_id = tweet_id
            file_to_write.write(unicode(tweet) + '\n')

        # it might be the case that there are > 200 new tweets
        # in this case, we'll need to paginate to get them.

        # pass max_id - 1 to backfill. subtract 1 as max_id is <=, not <
        arg_dict['max_id'] = minimum_id - 1

    file_to_write.close()


def perform_api_user_timeline_call(api, arg_dict):
    current_page = None
    while current_page == None:
        try:
            current_page = api.GetUserTimeline(**arg_dict)
        except twitter.TwitterError as te:
            if te.message == "Not authorized":
                print "Not authorized to grab this person's tweets. Moving on."
                break
            else:
                print "Reached API rate limit. Cooling off for %d seconds." % constants.API_COOL_OFF_SECONDS
                print te
                time.sleep(constants.API_COOL_OFF_SECONDS)
        except Exception as e:
            print "This person broke the API. Remove, please."
            print "Exception: %s" % e
            break
    if current_page == None:
        return []
    else:
        return current_page

def backfill(user):
    print "Backfilling: %s" % user
    _api = twitter.Api(consumer_key=constants.consumer_key, consumer_secret=constants.consumer_secret,
        access_token_key=constants.access_token_key, access_token_secret=constants.access_token_secret)
    last_tweet_id = 0

    results_to_write = []

    while True:

        arg_dict = {'id': user, 'count': 200, 'include_entities': True, 'include_rts': True}

        if last_tweet_id != 0:
            arg_dict['max_id'] = last_tweet_id - 1  # avoid duplicates

        current_page = perform_api_user_timeline_call(_api, arg_dict)

        # have we received all the tweets we can?
        if len(current_page) == 0:
            break

        for tweet in current_page:
            results_to_write.append(unicode(tweet) + '\n')

        last_tweet_id = current_page[-1].GetId()

    if len(results_to_write) > 0:
        file_to_write = codecs.open(os.path.join(constants.user_directory,
            convert_user_to_file(user)), mode='wb', encoding='utf-8')
        file_to_write.writelines(results_to_write)
        file_to_write.close()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="""Get the complete timeline of a user as currently exists in Twitter""")
    parser.add_argument('-user', help='Give Twitter account name. Pass in with an @ sign or not.')
    parser.add_argument('--all', action='store_true', help='Run for all users in corpus.')
    parser.add_argument('--force', action='store_true', help='Force a backfill.')
    parser.add_argument('-file', help=""" Pass a csv file. Assume there's
        a column named 'Twitter Account'. """)
    parser.add_argument('--new_only', action='store_true', help='Only process new accounts (no updates).')

    args = vars(parser.parse_args())
    if args['file']:
        process_file(args['file'], force_backfill=args['force'], new_only=args['new_only'])
    elif args['all']:
        update_all_users(force_backfill=args['force'], new_only=args['new_only'])
    elif args['user'] is not None:
        handle_single_user(args['user'], force_backfill=args['force'], new_only=args['new_only'])
    else:
        raise Exception("Nothing input!")
