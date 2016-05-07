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
    records.cache()

    print "Mapping of first categorical feasture column: %s" % get_mapping(records, 2)

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    cat_features = dict([(i - 2, len(get_mapping(records, i)) + 1) for i in range(2,10)])
    print "Categorical feature size mapping %s" % cat_features
    # train the model again
    dt_model_2 = DecisionTree.trainRegressor(data_dt, categoricalFeaturesInfo=cat_features)
    preds_2 = dt_model_2.predict(data_dt.map(lambda p: p.features))
    actual_2 = data.map(lambda p: p.label)
    true_vs_predicted_dt_2 = actual_2.zip(preds_2)

    calculate_print_metrics("Decision Tree Categorical", true_vs_predicted_dt_2)


if __name__ == "__main__":
    main()

