package org.sparksamples

import scalax.chart._

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object UserAgesChart {

  def main(args: Array[String]) {
    val user_data = Util.getUserData()
    val user_fields = user_data.map(l => l.split("\\|"))
    val ages = user_fields.map( x => (x(1).toInt)).collect()
    println(ages.getClass.getName)
    val m = {}
    m
    var zero = 0
    var ten = 0
    var twenty = 0
    var thirty = 0
    var forty = 0
    var fifty = 0
    var sixty = 0
    var seventy = 0
    var eighty = 0
    var ninty = 0

    for(i <- 0 until ages.length){
      println("i is: " + i);
      println("i'th element is: " + ages(i));
      if(ages(i) < 10){
        zero = zero + 1
      }else if (ages(i) >= 10 && ages(i) < 20) {
        ten = ten + 1
      } else if (ages(i) >= 20 && ages(i) < 30) {
        twenty = twenty + 1
      } else if (ages(i) >= 30 && ages(i) < 40) {
        thirty = thirty + 1
      } else if (ages(i) >= 40 && ages(i) < 50) {
        forty = forty + 1
      } else if (ages(i) >= 50 && ages(i) < 60) {
        fifty = fifty + 1
      } else if (ages(i) >= 60 && ages(i) < 70) {
        sixty = sixty + 1
      } else if (ages(i) >= 70 && ages(i) < 80) {
        seventy = seventy + 1
      } else if (ages(i) >= 80 && ages(i) < 90) {
        eighty = eighty + 1
      }else if (ages(i) >= 90 ) {
        ninty = ninty + 1
      }

    }

    val ds = new org.jfree.data.category.DefaultCategoryDataset
    //ages foreach (x => ds.addValue(x._1,"User Ages", x._2))
    ds.addValue(zero,"User Ages",0)
    ds.addValue(ten,"User Ages",10)
    ds.addValue(twenty,"User Ages",20)
    ds.addValue(thirty,"User Ages",30)
    ds.addValue(forty,"User Ages",40)
    ds.addValue(fifty,"User Ages",50)
    ds.addValue(sixty,"User Ages",60)
    ds.addValue(seventy,"User Ages",70)
    ds.addValue(eighty,"User Ages",80)
    ds.addValue(ninty,"User Ages",90)
    val chart = ChartFactories.BarChart(ds)
    chart.show()
    Util.sc.stop()
  }
}
