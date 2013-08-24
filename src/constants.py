import os.path
import time

# all the important information has been taken out.
# fill in your own fields!

consumer_key = 'SOMETHING'
consumer_secret = 'SOMETHING'
access_token_key = 'SOMETHING'
access_token_secret = 'SOMETHING'

election_utc_time = time.strptime("Tue Nov 06 00:00:00 +0000 2012",
    '%a %b %d %H:%M:%S +0000 %Y')

API_COOL_OFF_SECONDS = 10 * 60

TWEET_TRAINING_TEST_SPLIT = 0.8
ACCOUNT_TRAINING_TEST_SPLIT = 0.9

TEMPORAL_SPLIT = time.strptime("Mar 01 00:00:00 +0000 2012",
    "%b %d %H:%M:%S +0000 %Y")

mysql_user = 'SOMETHING'
mysql_password = 'SOMETHING'
mysql_database = 'SOMETHING'
mysql_charset = 'SOMETHING'

LINUX_BASE = 'SOMETHING'
MAC_BASE = 'SOMETHING'

BASE_DIR = LINUX_BASE if os.path.exists(LINUX_BASE) else MAC_BASE

user_directory = os.path.join(LINUX_BASE, 'users')

decision_boundary_pickling_file = os.path.join(BASE_DIR, "pickled/decision_boundary")
supervised_classifier_pickling_file = os.path.join(BASE_DIR, "pickled/classifier")
tfidf_vectorizer_pickling_file = os.path.join(BASE_DIR, "pickled/tfidf_transformer")
training_data_pickling_file = os.path.join(BASE_DIR, "pickled/training_data")
training_labels_pickling_file = os.path.join(BASE_DIR, "pickled/training_labels")

NUMBER_SUBBED_TOKEN = "numbr"

OBAMA_VOTE = 'o'
ROMNEY_VOTE = 'r'

STEMMED_TWEETS_TABLE = "stemmed_tweets"
STEMMED_SPELL_CHECKED_TWEETS_TABLE = "stemmed_spell_checked_tweets"
