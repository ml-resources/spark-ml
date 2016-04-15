import os
import sys
from util import get_mapping
from util import extract_features
from util import extract_label
from util import extract_features_dt
from util import squared_error
from util import abs_error
from util import squared_log_error
from util import path
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import RidgeRegressionWithSGD
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.tree import DecisionTree
import numpy as np

os.environ['SPARK_HOME'] = "/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6/"
sys.path.append("/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6//python")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def main():
    sc = SparkContext(appName="PythonApp")
    raw_data = sc.textFile(path)
    num_data = raw_data.count()
    records = raw_data.map(lambda x: x.split(","))
    first = records.first()
    print first
    print num_data
    records.cache()

    print "Mapping of first categorical feasture column: %s" % get_mapping(records, 2)

    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    print "Feature vector length for categorical features: %d" % cat_len
    print "Feature vector length for numerical features: %d" % num_len
    print "Total feature vector length: %d" % total_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    first_point = data.first()

    print "Linear Model feature vector:\n" + str(first_point.features)
    print "Linear Model feature vector length: " + str(len(first_point.features))

    linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
    true_vs_predicted = data.map(lambda p: (p.label, linear_model.predict(p.features)))
    print "Linear Model predictions: " + str(true_vs_predicted.take(5))

    mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
    mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
    rmsle = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_log_error(t, p)).mean())
    print "Linear Model - Mean Squared Error: %2.4f" % mse
    print "Linear Model - Mean Absolute Error: %2.4f" % mae
    print "Linear Model - Root Mean Squared Log Error: %2.4f" % rmsle


if __name__ == "__main__":
    # execute only if run as a script
    main()

