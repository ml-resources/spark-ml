import os
import sys

import pylab as P
import matplotlib
import matplotlib.pyplot as plt

from com.sparksamples.util import evaluate
from com.sparksamples.linearregression.LinearRegressionUtil import get_train_test_data


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

os.environ['SPARK_HOME'] = "/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6/"
sys.path.append("/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6//python")


def main():
    execute()


def execute():

    train_data, test_data = get_train_test_data()
    params = [0.0, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
    metrics = [evaluate(train_data, test_data, 10, 0.1, param, 'l1', False) for param in params]
    print metrics
    P.plot(params, metrics)
    fig = matplotlib.pyplot.gcf()
    plt.xscale('log')
    P.show()


if __name__ == "__main__":
    main()

