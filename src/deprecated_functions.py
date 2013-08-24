def construct_training_and_test_data(corpus, tweet_data, labels, hashtags=None):

    print "MANUAL SPARSE MATRIX CONSTRUCTION"

    # map words to feature indices.
    word_dictionary = dict((word, i) for i, word in enumerate(corpus))

    # segment data into training/test matricies
    # to pre-allocate sparse matrices to appropriate lengths
    training_rows = set()
    test_rows = set()

    RATIO = constants.TRAINING_TEST_SPLIT    # the training/test data split
    for i in xrange(len(tweet_data)):
        value = numpy.random.rand()
        if value < RATIO:
            training_rows.add(i)
        else:
            test_rows.add(i)

    training_data = scipy.sparse.lil_matrix((len(training_rows), len(word_dictionary)))
    training_counter = 0

    test_data = scipy.sparse.lil_matrix((len(test_rows), len(word_dictionary)))
    test_counter = 0

    for i, tweet in enumerate(tweet_data):
        stemmed_words = tweet[0].split()
        account_id = tweet[1]
        tweet_id = tweet[2]

        if i in training_rows:
            processed_data = training_data
            counter = training_counter
            training_counter += 1
        else:
            processed_data = test_data
            counter = test_counter
            test_counter += 1

        for stemmed_word in stemmed_words:
            if stemmed_word in word_dictionary:
                processed_data[counter, word_dictionary[stemmed_word]] += 1

        if hashtags != None and tweet_id in hashtags:
            for hashtag in hashtags[tweet_id]:
                if hashtag in word_dictionary:
                    processed_data[counter, word_dictionary[hashtag]] += 1

    training_labels, test_labels = \
        construct_training_and_test_label_vectors(tweet_data, labels, training_rows, test_rows)

    return training_data, training_labels, test_data, test_labels

def tfidf_transformation(training_data, testing_data):
    
    print "TF-IDF TRANSFORMATION"
    
    training_transformer = sklearn.feature_extraction.text.TfidfTransformer()
    training_transformer.fit(training_data)
    transformed_training_data = training_transformer.transform(training_data)

    # use parameters from the training set for the sample set.
    transformed_test_data = training_transformer.transform(testing_data)

    return transformed_training_data, transformed_test_data

def construct_word_corpus(cursor, hashtags=None):

    corpus = set()

    '''cursor.execute(""" select s.stemmed_text, t.tweet_id from stemmed_tweets s,
        tweets t, anchors a, industries i where t.tweet_id = s.tweet_id and
        t.account_id = a.account_id and a.industry_id = i.industry_id and 
        i.industry in ('politics') """)
    '''
    cursor.execute(""" select s.stemmed_text, t.tweet_id from stemmed_tweets s,
        tweets t where t.tweet_id = s.tweet_id """)

    tokenizer = sklearn.feature_extraction.text.CountVectorizer().build_tokenizer()

    for row in cursor.fetchall():

        split_words = tokenizer(row[0])
        tweet_id = row[1]
        if hashtags != None and tweet_id in hashtags:
            corpus.update(hashtags[tweet_id])
        corpus.update(split_words)

    return corpus

