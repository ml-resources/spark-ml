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
from pyspark.mllib.tree import GradientBoostedTrees


import numpy as np
# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6/"

# Append to PYTHONPATH so that pyspark could be found
sys.path.append("/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6//python")
# Now we are ready to import Spark Modules
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
    # we want to extract the feature mappings for columns 2 - 9
    # try it out on column 2 first
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
    first_point = data.first()

    gbt_model = GradientBoostedTrees.trainRegressor(data,categoricalFeaturesInfo={}, numIterations=3)
    true_vs_predicted_gbt = data.map(lambda p: (p.label, gbt_model.predict(p.features)))

    predictions = gbt_model.predict(data.map(lambda x: x.features))
    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
    mse = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /\
        float(data.count())
    mae = labelsAndPredictions.map(lambda (v, p): np.abs(v - p)).sum() /\
        float(data.count())
    rmsle = labelsAndPredictions.map(lambda (v,p) :  ((np.log(p + 1) - np.log(v + 1))**2)).sum() /\
        float(data.count())
    print('Gradient Boosted Trees - Mean Squared Error = ' + str(mse))
    print('Gradient Boosted Trees - Mean Absolute Error = ' + str(mae))
    print('Gradient Boosted Trees - Mean Root Mean Squared Log Error = ' + str(rmsle))


if __name__ == "__main__":
    main()

