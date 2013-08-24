import constants
import construct_matrices
import diagnostics
import pickle
import pull_from_db


def classify(training_data, training_labels, testing_data, testing_labels,
    return_predictions=False, store_classifier=False, verbose=False, store_data=False):
    
    if verbose: print "Start Learning...."
    
    from sklearn.svm import LinearSVC
    clf = LinearSVC()

    clf.fit(training_data, training_labels)
    
    if verbose: print "Done."

    print "Data-Level Testing Set Prediction Accuracy: %s" % clf.score(testing_data, testing_labels)
    print "Data-Level Training Set Prediction Accuracy: %s" % clf.score(training_data, training_labels)

    if store_classifier:
        if verbose: print "Pickling the trained classifier..."
        pickle.dump(clf, open(constants.supervised_classifier_pickling_file, 'wb'))
        if verbose: print "Done"

    if store_data:
        if verbose: print "Pickling fitted coefficients of classifier..."
        coefficients = clf.intercept_.tolist()
        coefficients.extend(clf.coef_.flatten().tolist())
        pickle.dump(coefficients, open(constants.decision_boundary_pickling_file, 'wb'))
        if verbose: print "Done"

    if return_predictions:
        return clf.predict(training_data), clf.predict(testing_data)


def check_account_level_accuracy(predictions, labels, index_mappings, tweet_data):

    account_predictions = dict()

    for i, tweet in enumerate(tweet_data):

        if i not in index_mappings:
            continue

        account_id = tweet[1]
        prediction = predictions[index_mappings[i]]

        if account_id not in account_predictions:
            account_predictions[account_id] = 0

        account_predictions[account_id] += prediction

    misclassifications = 0
    for account_id in account_predictions:
        account_predictions[account_id] = -1 if account_predictions[account_id] < 0 else 1
        if account_predictions[account_id] != labels[account_id]:
            misclassifications += 1


    print "Number of Accounts: %s" % len(account_predictions)
    print "Unweighted Account-Level Prediction Accuracy: %s" % \
        (1 - float(misclassifications) / len(account_predictions))

    diagnostics.generate_diagnostics(labels, account_predictions)


def store_training_data(data_matrix, data_labels):
    '''
        Pickle training data and labels. This is normally done to
        later plot the data (see plot_2D_data.py for details).
    '''
    pickle.dump(data_matrix, open(constants.training_data_pickling_file, 'wb'))
    pickle.dump(data_labels, open(constants.training_labels_pickling_file, 'wb'))


def run(use_non_stemmed_text, sample_size=None,
    store_classifier=False, use_hashtags=False, verbose=False, pca=False,
    store_data=False, training_size=-1, account_size=-1, spell_check=False,
    account_grouping=False):

    # pull the voting classification of each account.
    labels = pull_from_db.pull_anchor_classifications()

    # pull raw data. either hashtags or tweet data (which can be stemmed or raw)
    if use_hashtags:
        if account_grouping:
            tweet_data = pull_from_db.pull_account_grouped_hashtags(labels)
        else:
            tweet_data = pull_from_db.pull_tweet_hashtags(labels,
                predefined_training_size=sample_size)
    else:
        if account_grouping:
            tweet_data = pull_from_db.pull_account_grouped_tweets(labels,
                stemmed=not use_non_stemmed_text, predefined_training_size=sample_size,
                use_spell_checked_text=spell_check)
        else:
            tweet_data = pull_from_db.pull_tweets(labels,
                stemmed=not use_non_stemmed_text,
                predefined_training_size=sample_size,
                use_spell_checked_text=spell_check)  

    # product training and test matrices.
    training_data, training_labels, training_index_mapping, \
    testing_data, testing_labels, test_index_mapping = \
        construct_matrices.construct_training_and_test_matrices(tweet_data, labels,
            using_hashtags=use_hashtags, store=store_classifier,
            verbose=verbose, pca=pca, defined_training_size=training_size, defined_account_numbers=account_size)

    print "Samples: %d" % (testing_data.shape[0] + training_data.shape[0])
    print "Test Matrix size: %s x %s" % testing_data.shape
    print "Training Matrix size: %s x %s" % training_data.shape

    if store_data:
        store_training_data(training_data, training_labels)

    training_predictions, test_predictions = classify(training_data, training_labels, testing_data,
        testing_labels, return_predictions=True, store_classifier=store_classifier,
        verbose=verbose, store_data=store_data)

    print "Account-Level Accuracy of Testing Data:"
    check_account_level_accuracy(test_predictions, labels, test_index_mapping, tweet_data)

    print "Account-Level Accuracy of Training Data:"
    check_account_level_accuracy(training_predictions, labels, training_index_mapping, tweet_data)


if __name__ == '__main__':
    import argparse
    import time

    start = time.time()

    parser = argparse.ArgumentParser(description="Classify tweet data using a supervised learning algorithm.")
    parser.add_argument('--sample_size', type=int, help='Specify sample size.')
    parser.add_argument('--training_size', type=int, default=-1, help='Specify sample size.')
    parser.add_argument('--non_stem', action='store_true', help='Use non-stemmed text.')
    parser.add_argument('--store_objects', action='store_true', help='Pickle objects to do online prediction later.')
    parser.add_argument('--hashtags', action='store_true', help='Use hashtags.')
    parser.add_argument('--verbose', action='store_true', help='Print logging data.')
    parser.add_argument('-pca', type=int, help='Perform PCA on data matrices. Pass # of components to compress to.')
    parser.add_argument('--store_data', action='store_true', help='Store training data for later use.')
    parser.add_argument('--training_accounts', type=int, default=-1, help='Specify number of accounts to use in training.')
    parser.add_argument('--spell_check', action='store_true', help='Use spell-checked, stemmed text.')
    parser.add_argument('--account_grouping', action='store_true', help='Group data by account.')

    args = vars(parser.parse_args())
    run(args['non_stem'], args['sample_size'], args['store_objects'],
        args['hashtags'], args['verbose'], args['pca'], args['store_data'],
        args['training_size'], args['training_accounts'], args['spell_check'],
        args['account_grouping'])

    end = time.time()
    print "Runtime: %f seconds" % (end - start)
