import constants
import get_complete_user_timeline_before_election as gcutbe
import pickle
import pull_from_db as pfd
import push_file_into_db as pfid
import preprocess_tweets as pt


def run(username, force_stem=False):

    # process data first.
    gcutbe.handle_single_user(username)
    user_file = gcutbe.get_file_for_user(gcutbe.convert_user_to_file(username))
    pfid.run(user_file)
    pt.run(username=username, force_user_stem=force_stem)

    # get data for processing.
    tweets = pfd.pull_tweets_from_user(username, stemmed=True)
    transformer = pickle.load(
        open(constants.tfidf_vectorizer_pickling_file, 'rb'))

    testing_data = transformer.transform(tweet[0] for tweet in tweets)

    classifier = pickle.load(
        open(constants.supervised_classifier_pickling_file, 'rb'))
    unweighted_predictions = classifier.predict(testing_data)
    total_prediction = sum(unweighted_predictions)

    print "Prediction: %s" % \
        ('Obama' if total_prediction < 0 else 'Romney')
    print "Prediction Confidence: %s" % \
        (float(total_prediction) / len(unweighted_predictions))

if __name__ == '__main__':
    import argparse
    import time

    start = time.time()

    parser = argparse.ArgumentParser(
        description="""Classify tweet data for one person using
        a supervised learning algorithm.""")
    parser.add_argument('-user', type=str, required=True,
        help='User to predict for.')
    parser.add_argument('--force_stem', action='store_true',
        help='Force user tweet stemming.')
    args = vars(parser.parse_args())

    run(args['user'], args['force_stem'])

    end = time.time()
    print "Runtime: %f seconds" % (end - start)
