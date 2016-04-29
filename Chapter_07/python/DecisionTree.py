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
    records.cache()

    print "Mapping of first categorical feasture column: %s" % get_mapping(records, 2)

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    print "Feature vector length for categorical features: %d" % cat_len
    print "Feature vector length for numerical features: %d" % num_len
    print "Total feature vector length: %d" % total_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    first_point_dt = data_dt.first()
    print "Decision Tree feature vector: " + str(first_point_dt.features)
    print "Decision Tree feature vector length: " + str(len(first_point_dt.features))

    dt_model = DecisionTree.trainRegressor(data_dt, {})
    preds = dt_model.predict(data_dt.map(lambda p: p.features))
    actual = data.map(lambda p: p.label)
    true_vs_predicted_dt = actual.zip(preds)
    print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
    print "Decision Tree depth: " + str(dt_model.depth())
    print "Decision Tree number of nodes: " + str(dt_model.numNodes())


    mse_dt = true_vs_predicted_dt.map(lambda (t, p): squared_error(t, p)).mean()
    mae_dt = true_vs_predicted_dt.map(lambda (t, p): abs_error(t, p)).mean()
    rmsle_dt = np.sqrt(true_vs_predicted_dt.map(lambda (t, p): squared_log_error(t, p)).mean())
    print "Decision Tree - Mean Squared Error: %2.4f" % mse_dt
    print "Decision Tree - Mean Absolute Error: %2.4f" % mae_dt
    print "Decision Tree - Root Mean Squared Log Error: %2.4f" % rmsle_dt

if __name__ == "__main__":
    main()

