package org.sparksamples

import scalax.chart._
import scala.collection.immutable.ListMap

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object UserAgesChart {

  def main(args: Array[String]) {
    val user_data = Util.getUserData()
    val user_fields = user_data.map(l => l.split("\\|"))
    val ages = user_fields.map( x => (x(1).toInt)).collect()
    println(ages.getClass.getName)

    var m = Map(0 -> 0,
      10 -> 0,
      20 -> 0, 30 -> 0, 40 -> 0, 50 -> 0, 60 -> 0, 70 -> 0, 80 -> 0
    )

    for(i <- 0 until ages.length){
      if(ages(i) < 10){
        m = m + (0 -> (m(0) + 1))
      }else if (ages(i) >= 10 && ages(i) < 20) {
        m = m + (10 -> (m(10) + 1))
      } else if (ages(i) >= 20 && ages(i) < 30) {
        m = m + (20 -> (m(20) + 1))
      } else if (ages(i) >= 30 && ages(i) < 40) {
        m = m + (30 -> (m(30) + 1))
      } else if (ages(i) >= 40 && ages(i) < 50) {
        m = m + (40 -> (m(40) + 1))
      } else if (ages(i) >= 50 && ages(i) < 60) {
        m = m + (50 -> (m(50) + 1))
      } else if (ages(i) >= 60 && ages(i) < 70) {
        m = m + (60 -> (m(60) + 1))
      } else if (ages(i) >= 70 && ages(i) < 80) {
        m = m + (70 -> (m(70) + 1))
      } else if (ages(i) >= 80 && ages(i) < 90) {
        m = m + (80 -> (m(80) + 1))
      }else if (ages(i) >= 90 ) {
        m = m + (90 -> (m(90) + 1))
      }

    }
    println(m)
    val m_sorted =  ListMap(m.toSeq.sortBy(_._1):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    m_sorted.foreach{ case (k,v) => ds.addValue(v,"UserAges", k)}

    val chart = ChartFactories.BarChart(ds)

    chart.show()
    Util.sc.stop()
  }
}
