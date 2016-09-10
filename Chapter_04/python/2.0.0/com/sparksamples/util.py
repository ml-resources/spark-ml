import os
import sys
from pyspark.sql.types import *
PATH = "/home/ubuntu/work/ml-resources/spark-ml/data"
SPARK_HOME = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/"

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def get_user_data():
    custom_schema = StructType([
        StructField("no", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("zipCode", StringType(), True)
    ])
    from pyspark.sql import SQLContext
    from pyspark.sql.types import *

    sql_context = SQLContext(sc)

    user_df = sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header='false', delimiter='|') \
        .load("%s/ml-100k/u.user" % PATH, schema = custom_schema)
    return user_df


def get_movie_data():
    return sc.textFile("%s/ml-100k/u.item" % PATH)

def get_rating_data():
    return sc.textFile("%s/ml-100k/u.data" % PATH)


