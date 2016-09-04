package org.sparksamples

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
/**
  * Created by Rajdeep on 12/22/15.
  */

object MovieData {

  def main(args: Array[String]) {

    val PATH = "/home/ubuntu/work/ml-resources/spark-ml/data/ml-100k/u.item"
    import org.apache.spark.sql.Row

    val sqlContext = new SQLContext(org.sparksamples.Util.sc)

    val rowRdd = sqlContext.sparkContext.textFile(PATH).map { line =>
      val tokens = line.split('|')
      Row(org.sparksamples.Util.convertYear(tokens(2)))
    }
    val fields = Seq(
      StructField("year", IntegerType, true)
    )
    val schema = StructType(fields)
    val years = sqlContext.createDataFrame(rowRdd, schema)
    println(years.first())
    val years_filtered = years.filter(x => (x != 1900) )
    years_filtered.foreachPartition(print(_))

    def convert2(x: String): Integer = {
      val y = Util.convert(x)
      return 1998 - y
    }

    val rowRdd_ages = sqlContext.sparkContext.textFile(PATH).map { line =>
      val tokens = line.split('|')
      Row(
        Util.convert(
          org.sparksamples.Util.convertYear(tokens(2))
        )
      )
    }

    val movies_ages = sqlContext.createDataFrame(rowRdd_ages, schema)

    print(movies_ages.first())

    val movies_ages_sorted = movies_ages.groupBy("year").count().sort("year")
    movies_ages_sorted.show()

    println()

  }



}