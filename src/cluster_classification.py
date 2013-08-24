import construct_matrices
import pull_from_db
import time


def cluster_samples(data, clusters):

    print "Clustering samples into %s groups ... " % clusters

    from sklearn.cluster import KMeans

    clusterizer = KMeans(n_clusters=clusters, n_jobs=-1)

    return clusterizer.fit_predict(data)


def run(k, predefined_training_size=None, use_stemmed_text=False):

    labels = pull_from_db.pull_anchor_classifications()
    # TOO MUCH DATA! Skip full text.
    #tweet_data = pull_from_db.pull_tweets(labels, stemmed=use_stemmed_text,
    #    predefined_training_size=predefined_training_size)
    tweet_data = pull_from_db.pull_tweet_hashtags(labels)

    print "Samples: %d" % len(tweet_data)

    training_data, training_labels, training_tweet_mapping, \
    testing_data, testing_labels, test_tweet_mapping = \
        construct_matrices.construct_training_and_test_matrices(tweet_data,
            labels)

    print "Test Matrix size: %s x %s" % testing_data.get_shape()
    print "Training Matrix size: %s x %s" % training_data.get_shape()

    print "Clustering samples into %s groups ... " % k

    from sklearn.cluster import MiniBatchKMeans

    clusterizer = MiniBatchKMeans(n_clusters=k, init_size=2 * k)

    new_training_data = clusterizer.fit_predict(training_data)

    # determine the majority vote for the cluster.
    cluster_vote = dict()
    for i, n in enumerate(new_training_data):
        if n not in cluster_vote:
            cluster_vote[n] = 0
        cluster_vote[n] += training_labels[i]

    for cluster in cluster_vote:
        cluster_vote[cluster] = -1 if cluster_vote[cluster] < 0 else 1

    # based on the cluster vote, determine how polarized
    # that cluster is based on the tweets that were assigned
    # to said cluster. output the accuracy.
    misclassifications = 0
    for i, n in enumerate(new_training_data):
        if training_labels[i] != cluster_vote[n]:
            misclassifications += 1

    print "Misclassifications in Training Set: %s" % \
        (1 - float(misclassifications) / len(new_training_data))

    new_test_data = clusterizer.predict(testing_data)
    misclassifications = 0
    for i, n in enumerate(new_test_data):
        if testing_labels[i] != cluster_vote[n]:
            misclassifications += 1

    print "Misclassifications in Test Set: %s" % \
        (1 - float(misclassifications) / len(new_test_data))


if __name__ == '__main__':
    import argparse
    start = time.time()

    parser = argparse.ArgumentParser(description="Unsupervised learning.")

    parser.add_argument('-k', type=int, required=True,
        help='Number of centroids to use.')

    parser.add_argument('-size', type=int,
        help='Specify size of training size.')

    parser.add_argument('--stemmed', action='store_true',
        help='Use stemmed text.')

    args = vars(parser.parse_args())
    run(args['k'], args['size'], args['stemmed'])

    end = time.time()
    print "Runtime: %f seconds" % (end - start)
