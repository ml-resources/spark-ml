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
from pyspark.mllib.regression import RidgeRegressionWithSGD

import numpy as np

# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6/"

# Append to PYTHONPATH so that pyspark could be found
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

    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    print "Feature vector length for categorical features: %d" % cat_len
    print "Feature vector length for numerical features: %d" % num_len
    print "Total feature vector length: %d" % total_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    first_point = data.first()

    rr_model = RidgeRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
    true_vs_predicted_rr = data.map(lambda p: (p.label, rr_model.predict(p.features)))

    print "Ridge Regression Model predictions: " + str(true_vs_predicted_rr.take(5))

    mse_rr = true_vs_predicted_rr.map(lambda (t, p): squared_error(t, p)).mean()
    mae_rr = true_vs_predicted_rr.map(lambda (t, p): abs_error(t, p)).mean()
    rmsle_rr = np.sqrt(true_vs_predicted_rr.map(lambda (t, p): squared_log_error(t, p)).mean())
    print "Ridge Regression - Mean Squared Error: %2.4f" % mse_rr
    print "Ridge Regression - Mean Absolute Error: %2.4f" % mae_rr
    print "Ridge Regression - Root Mean Squared Log Error: %2.4f" % rmsle_rr


if __name__ == "__main__":
    main()

