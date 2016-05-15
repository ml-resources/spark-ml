import os
import sys

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import RidgeRegressionWithSGD

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import get_records
from com.sparksamples.util import calculate_print_metrics



# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/home/ubuntu/work/spark-1.6.1-bin-hadoop2.6/"

# Append to PYTHONPATH so that pyspark could be found
sys.path.append("/home/ubuntu/work/spark-1.6.1-bin-hadoop2.6//python")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def main():
    records = get_records()
    records.cache()

    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    rr_model = RidgeRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
    true_vs_predicted_rr = data.map(lambda p: (p.label, rr_model.predict(p.features)))

    print "Ridge Regression Model predictions: " + str(true_vs_predicted_rr.take(5))

    calculate_print_metrics("Ridge Regression", true_vs_predicted_rr)


if __name__ == "__main__":
    main()

