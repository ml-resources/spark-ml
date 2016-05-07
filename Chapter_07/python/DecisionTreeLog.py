import os
import sys
from util import get_mapping
from util import extract_features
from util import extract_label
from util import extract_features_dt

from util import get_records
from util import calculate_print_metrics
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
    records = get_records()
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

    data_dt_log = data_dt.map(lambda lp: LabeledPoint(np.log(lp.label), lp.features))
    dt_model_log = DecisionTree.trainRegressor(data_dt_log, {})

    preds_log = dt_model_log.predict(data_dt_log.map(lambda p: p.features))
    actual_log = data_dt_log.map(lambda p: p.label)
    true_vs_predicted_dt_log = actual_log.zip(preds_log).map(lambda (t, p): (np.exp(t), np.exp(p)))

    calculate_print_metrics("Decision Tree Log", true_vs_predicted_dt_log)


if __name__ == "__main__":
    main()

