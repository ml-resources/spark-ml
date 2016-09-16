package org.sparksamples

import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  * Modified on Sep 14 2016
  */
object MovieAgesChart {

  def main(args: Array[String]) {
    /*val movie_data_df = Util.getMovieDataDF()
    movie_data_df.createOrReplaceTempView("movie_data")


    movie_data_df.printSchema()

    Util.spark.udf.register("convertYear", Util.convertYear _)


    val movie_years = Util.spark.sql("select convertYear(date) as year from movie_data")
    val movie_years_count = movie_years.groupBy("year").count()
    movie_years_count.show()
    val movie_years_count_rdd = movie_years_count.rdd.map(row => (Integer.parseInt(row(0).toString), row(1).toString))
    val movie_years_count_collect = movie_years_count_rdd.collect()
    val movie_years_count_collect_sort = movie_years_count_collect.sortBy(_._1)*/

    val movie_years_count_collect_sort = MovieData.getMovieYearsCountSorted()

    val ds = new org.jfree.data.category.DefaultCategoryDataset
    for(i <- movie_years_count_collect_sort){
      ds.addValue(i._2.toDouble,"year", i._1)
    }

    val chart = ChartFactories.BarChart(ds)
    chart.show()
    Util.sc.stop()
  }
}
