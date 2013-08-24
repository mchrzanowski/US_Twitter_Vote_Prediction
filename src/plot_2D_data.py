import constants
import matplotlib.pyplot as plt
import pickle
import time


def run():
    '''
        This method will plot 2D data generated from a run
        of the supervised learning pipeline.
        Dependencies:
            * a data matrix of 2 columns stored in:
                constants.training_data_pickling_file
            * a corresponding label vector for each row of
                the data matrix stored in:
                constants.training_labels_pickling_file
                It is assumed that -1 indicates an Obama vote.
            * the 3 coefficients resulting from a classifier
                trained on the data matrix stored in:
                constants.decision_boundary_pickling_file
        Output:
            * a plot of the data color-coded by vote and with
                the decision boundary drawn
    '''

    obamaX = []
    obamaY = []
    romneyX = []
    romneyY = []

    data_matrix = pickle.load(
        open(constants.training_data_pickling_file, 'rb'))
    data_labels = pickle.load(
        open(constants.training_labels_pickling_file, 'rb'))

    for i, row in enumerate(data_matrix):
        x = data_matrix[i, 0]
        y = data_matrix[i, 1]
        if data_labels[i] == -1:    # obama vote.
            obamaX.append(x)
            obamaY.append(y)
        else:
            romneyX.append(x)
            romneyY.append(y)

    # it just takes 2 points to plot the decision boundary.
    min_x1 = min(min(obamaX), min(romneyX))
    max_x1 = max(max(obamaX), max(romneyX))

    coefficients = pickle.load(
        open(constants.decision_boundary_pickling_file, 'rb'))

    # x0 = intercept
    # x1 = coefficient for first column
    # x2 = coefficient for second column
    x0, x1, x2 = coefficients

    prediction = lambda x: float(-x0 + -x1 * x) / x2

    # construct the predicted points for each x value.
    decisionX = [min_x1, max_x1]
    decisionY = [prediction(min_x1), prediction(max_x1)]

    plt.scatter(obamaX, obamaY, s=1, color='blue')
    plt.scatter(romneyX, romneyY, s=1, color='red')
    plt.plot(decisionX, decisionY, color='green')

    plt.xlabel("X")
    plt.ylabel("Y")
    plt.show()


if __name__ == '__main__':
    start = time.time()
    run()
    end = time.time()
    print "Runtime: %f seconds" % (end - start)
