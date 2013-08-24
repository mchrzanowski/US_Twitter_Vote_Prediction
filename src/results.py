

def determine_prediction_accuracy_on_tweet_data(classifier, data, ground_truths):
    return classifier.score(data, ground_truths)


def check_account_level_accuracy(predictions, labels, tweet_mappings, tweet_data):

    account_predictions = dict()

    for i, tweet in enumerate(tweet_data):

        tweet_id = tweet[2]

        if tweet_id not in tweet_mappings:
            continue

        account_id = tweet[1]
        prediction = predictions[tweet_mappings[tweet_id]]

        if account_id not in account_predictions:
            account_predictions[account_id] = 0

        account_predictions[account_id] += prediction

    misclassifications = 0
    for account_id in account_predictions:
        prediction = -1 if account_predictions[account_id] < 0 else 1
        if prediction != labels[account_id]:
            misclassifications += 1

    return 1 - float(misclassifications) / len(account_predictions) 