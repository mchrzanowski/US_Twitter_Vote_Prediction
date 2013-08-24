import numpy
import sklearn.metrics


def find_best_parameters_for_SVM(training_data, training_labels, parameters=None):

    print "Starting Grid Search..."
    import sklearn.grid_search
    import sklearn.svm

    classifier = sklearn.svm.LinearSVC()

    if parameters == None:
        parameters = {'loss': ['l1', 'l2'],
            'C': [0.1, 0.2, 0.4, 0.8, 1, 2, 4, 8, 16]}

    print "Grid Parameters: %s" % parameters
    grd = sklearn.grid_search.GridSearchCV(classifier, parameters, n_jobs=-1)
    grd.fit(training_data, training_labels)

    print "Best: ", grd.best_params_
    print "Scores: ", grd.grid_scores_


def generate_diagnostics(labels, predictions):
    
    # create arrays for diagnostic calculations.
    ground_truths = numpy.zeros(len(predictions), dtype=numpy.int8)
    prediction_labels = numpy.zeros(len(predictions), dtype=numpy.int8)

    for i, account_id in enumerate(predictions):
        ground_truths[i] = labels[account_id]
        prediction_labels[i] = predictions[account_id]

    generate_f1_score(prediction_labels, ground_truths)
    generate_confusion_matrix(prediction_labels, ground_truths)


def generate_f1_score(prediction_labels, true_labels):
    print "F-1 Score: %s" % \
        sklearn.metrics.f1_score(true_labels, prediction_labels)

def generate_confusion_matrix(prediction_labels, true_labels):
    print "Confusion Matrix:"
    print sklearn.metrics.confusion_matrix(true_labels, prediction_labels)
