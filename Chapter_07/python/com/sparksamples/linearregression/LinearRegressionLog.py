import sys
import numpy as np

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import path
from com.sparksamples.util import calculate_print_metrics


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def main():
    #records = get_records()
    conf = SparkConf().setMaster("local").setAppName("My app")

    sc = SparkContext(conf =conf)
    raw_data = sc.textFile(path)
    num_data = raw_data.count()
    records = raw_data.map(lambda x: x.split(","))
    mappings = [get_mapping(records, i) for i in range(2,10)]

    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    data_log = data.map(lambda lp: LabeledPoint(np.log(lp.label), lp.features))
    model_log = LinearRegressionWithSGD.train(data_log, iterations=10, step=0.1)
    true_vs_predicted_log = data_log.map(lambda p: (np.exp(p.label), np.exp(model_log.predict(p.features))))
    calculate_print_metrics("Linear Regression Log", true_vs_predicted_log)



if __name__ == "__main__":
    main()

