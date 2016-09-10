import os
import sys
PATH = "/home/ubuntu/work/ml-resources/spark-ml/data"
SPARK_HOME = "/home/ubuntu/work/spark-1.6.2-bin-hadoop2.6/"

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)


def get_user_data():
    return sc.textFile("%s/ml-100k/u.user" % PATH)


def get_movie_data():
    return sc.textFile("%s/ml-100k/u.item" % PATH)

def get_rating_data():
    return sc.textFile("%s/ml-100k/u.data" % PATH)