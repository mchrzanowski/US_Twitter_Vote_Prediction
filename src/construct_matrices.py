import constants
import numpy
import partition_samples
import pull_from_db


def construct_tf_idf_matrices(tweet_data, training_rows, test_rows, store=False, verbose=False):

    if verbose: print "Sparse Matrix Construction..."
    
    from sklearn.feature_extraction.text import TfidfVectorizer

    # create tf-idf-normalized training and test matrices.
    # importantly, use the fitted parameters from creating the training matrix
    # to in turn create the normalized test matrix. this way, no information
    # regarding the test matrix leaks into our learning on the training set.
    
    #vectorizer = TfidfVectorizer(stop_words='english', min_df=2, max_features=29096)
    vectorizer = TfidfVectorizer(stop_words='english', min_df=2, max_features=len(training_rows))

    training_data = vectorizer.fit_transform(
        tweet[0] for i, tweet in enumerate(tweet_data) if i in training_rows)
    testing_data = vectorizer.transform(
        tweet[0] for i, tweet in enumerate(tweet_data) if i in test_rows)

    # construct mappings between the index in tweet_data
    # to indices in the training and test matrices.
    # here, we note that each tweet gets added incrementally
    # to either matrix.
    training_index_mapping = dict()
    testing_index_mapping = dict()

    training_counter = 0
    testing_counter = 0

    for i in xrange(len(tweet_data)):
        if i in training_rows:
            training_index_mapping[i] = training_counter
            training_counter += 1
        elif i in test_rows:
            testing_index_mapping[i] = testing_counter
            testing_counter += 1

    if store:
        if verbose: print "Pickling transformer..."

        import pickle
        pickle.dump(vectorizer,
            open(constants.tfidf_vectorizer_pickling_file, 'wb'))

        if verbose: print "Done"

    if verbose: print "Done"

    return training_data, training_index_mapping, testing_data, testing_index_mapping


def construct_training_and_test_matrices(tweet_data, labels,
    defined_training_size=-1, using_hashtags=False, store=False,
    verbose=False, pca=None, defined_account_numbers=-1):
    '''
        Construct data matrices that we'll use for training and testing the classifier.
        Input:
            * tweet_data = iterable of tuple(tweet data, account_id, tweet_id)
            * labels = dict of account_id => vote
            * store = Flag indicating whether to pickle the TF-IDF transformer
            * verbose = control verbosity
        
        Output:
            * training_data = a matrix composed of all features for tweet data in the training partition
            * testing_data = a matrix composed of all features for tweet data in the test partition
            * training_labels = a vector composed of labels associated to the training matrix rows
            * testing_labels = just like training_labels but for the test matrix
            * training_index_mapping = mapping between index in tweet_data to which index in the training matrix
                                        it corresponds to
            * testing_index_mapping = same as for training_index_mapping but for the test set
    '''

    if verbose: print "Constructing Training & Test Matrices..."
    
    training_rows, test_rows = partition_samples.random_tweet_partition(tweet_data, verbose)
    #training_rows, test_rows = partition_samples.temporal_partition(
    #   tweet_data, constants.TEMPORAL_SPLIT,
    #   predefined_training_size=defined_training_size, verbose=verbose,
    #   predefined_account_number=defined_account_numbers)
    #training_rows, test_rows = partition_samples.random_account_partition(tweet_data, verbose)

    training_data, training_index_mapping, testing_data, testing_index_mapping = \
        construct_tf_idf_matrices(tweet_data, training_rows, test_rows, store, verbose)

    training_labels, testing_labels = \
        construct_training_and_test_labels(tweet_data, labels,
            training_index_mapping, testing_index_mapping, verbose)

    if using_hashtags:
        training_data = add_follower_data(tweet_data, training_data, training_index_mapping, verbose)
        testing_data = add_follower_data(tweet_data, testing_data, testing_index_mapping, verbose)

    if pca != None:
        training_data, testing_data = perform_pca(training_data, testing_data, pca, verbose)

    if verbose: print "Done"

    return training_data, training_labels, training_index_mapping, \
        testing_data, testing_labels, testing_index_mapping


def perform_pca(training_data, testing_data, component_number, verbose=False):
    '''
        Perform PCA to compress the number of features in the training and test matrices.
        Input:
            * training data matrix of tweets -> features
            * testing data matrix of tweets -> features
            * the number of components to compress to
            * verbosity
        Output:
            * compressed training matrix
            * compressed testing matrix
    '''

    if verbose: print "Performing PCA Compression to %s Components ..." % component_number

    from sklearn.decomposition import PCA
    pca = PCA(n_components=component_number, whiten=True)

    pca.fit(training_data)
    training_data = pca.transform(training_data)
    testing_data = pca.transform(testing_data)

    if verbose: print "Done"

    return training_data, testing_data


def add_follower_data(tweet_data, data_matrix, tweet_index_mapping, verbose=False):
    ''' 
        Add follower data to the feature matrix.
        WARNING: This is a super memory-intensive process. This should only be done
            on really small matrices!
        Input:
            * tweet_data = iterable of tuple(tweet data, account_id, tweet_id)
            * data_matrix = a matrix where each row corresponds to a tweet
            * tweet_index_mapping = a mapping of the index of a tweet in tweet_data
                to a row in the matrix
        Output:
            * a re-shaped matrix that has additional columns corresponding to
                the follower data for the author
    '''
    if verbose: print "Adding Follower Data ..."

    follower_data, followers_per_account = pull_from_db.pull_follower_data()

    if len(follower_data) == 0:
        return data_matrix

    prev_shape = data_matrix.shape

    # convert to DOK for reshaping while preserving current matrix mapping.
    # (shaping to any other sparse matrix seems to fuck up the data horrifically)
    data_matrix = data_matrix.todok()

    # expand matrix by followers_per_account columns.
    data_matrix.resize((prev_shape[0], prev_shape[1] + followers_per_account))

    for tweet in tweet_data:
        account_id = tweet[1]
        tweet_id = tweet[2]

        if tweet_id not in tweet_index_mapping:
            continue

        if account_id not in follower_data:
            continue

        matrix_index = tweet_index_mapping[tweet_id]
        for i, value in enumerate(follower_data[account_id]):
            data_matrix[matrix_index, prev_shape[1] + i] = value
    
    if verbose: print "Done"

    return data_matrix


def construct_training_and_test_labels(tweet_data, labels, training_mapping, testing_mapping, verbose=False):
    ''' 
        Input:
            * tweet_data = iterable of tuple(tweet data, account_id, tweet_id)
            * labels = dict of account_id => vote
            * training_mapping = a dict that maps an index of tweet_data to an index in the training set and
                            training label vector
            * testing_mapping = a dict that maps an index of tweet_data to an index in the testing set and
                            testing label vector

        Output:
            * training_labels = array of len(training_rows) that maps the author's
                            vote to a specific tweet in the training set via training_mapping
            * test_labels =  array of len(test_rows) that maps the author's
                            vote to a specific tweet in the test via testing_mapping
    '''
    if verbose: print "Constructing Training & Test Label Arrays ..."

    # construct label vectors for use in supervised learning.
    training_labels = numpy.zeros(len(training_mapping), dtype=numpy.int8)
    test_labels = numpy.zeros(len(testing_mapping), dtype=numpy.int8)

    for i, tweet in enumerate(tweet_data):
        account_id = tweet[1]

        if i in training_mapping:
            training_labels[training_mapping[i]] = labels[account_id]

        elif i in testing_mapping:
            test_labels[testing_mapping[i]] = labels[account_id]

    if verbose: print "Done."

    return training_labels, test_labels
