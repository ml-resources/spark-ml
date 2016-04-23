import org.apache.spark.mllib.linalg.{Vector, Vectors}
/**
  * Created by rajdeep dua on 4/15/16.
  */
object Util {

  def extractFeatures(record : Array[String], cat_len: Int,
                      mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): Vector ={
    var cat_vec = Vectors.zeros(cat_len)
    val cat_array = cat_vec.toArray
    var i = 0
    var step = 0
    for(field <- 2 until 10){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString)
        }
      }catch {
        case e: Exception => print(e)
      }
      cat_array(idx.toInt + step) = 1
      i +=1
      step = step + m.size
    }
    cat_vec = Vectors.dense(cat_array)

    val record_2 = record.slice(10,14)

    val record_3 = Array.fill(record_2.length){0.0}
    for( i<- 0 until record_2.length){
      record_3(i) = record_2(i).toDouble
    }
    val total_len = cat_array.length + record_2.length
    val record_4 = Array.fill(total_len){0.0}

    for( i<- 0 until cat_array.length){
      record_4(i) = cat_array(i)
    }
    for( i<- 0 until record_2.length){
      record_4(11 + i) = record_2(i).toDouble
    }

    val final_vc = Vectors.dense(record_4)

    return final_vc
  }

  def extract_features_dt(record : Array[String]): Vector = {
    val cat_len = record.length
    var cat_vec = Vectors.zeros(cat_len)
    //var cat_array = cat_vec.toArray
    var cat_array = Array[Double](cat_len)
    for(field <- 2 until 14){
      //val m = mappings(i)
      var idx = 0
      try {
        //if (m.keySet.exists(_ == field.toString)) {
          val x = record(field).toDouble
          cat_array(idx) = x
          idx += 1

      }catch {
        case e: Exception => print(e)
      }

    }
    cat_vec = Vectors.dense(cat_array)

    return cat_vec
  }
    //return np.array(map(float, record[2:14]))
  def extractAvgFeature(record : Array[String], cat_len: Int,
                      mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): Double ={
    //var cat_vec = Vectors.zeros(cat_len)
    //val cat_array = cat_vec.toArray
    var sum = 0L
    //var count = 0
    var i = 0
    //var step = 0
    for(field <- 2 until 9){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString).toLong
          sum = sum + idx
        }
      }catch {
        case e: Exception => print(e)
      }
      //cat_vec
      //cat_array(idx.toInt + step) = 1
      i +=1
      //step = step + m.size

    }
    val avg = sum/i
    return avg
  }

  def extractTwoFeatures(record : Array[String], cat_len: Int,
                        mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): String ={

    var sumX = 0L
    var i = 0
    for(field <- 2 until 5){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString).toLong
          sumX = sumX + idx
        }
      }catch {
        case e: Exception => print(e)
      }
      i +=1
    }
    var j = 0
    val x = sumX/i
    var sumY = 0L
    for(field <- 6 until 9){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString).toLong
          sumY = sumY + idx
        }
      }catch {
        case e: Exception => print(e)
      }
      j +=1

    }
    val y = sumY/j

    return x + ", "  + y
  }

  def extractLabel(r: Array[String]): Double ={
    //print(r( r.length -1))
    return r( r.length -1).toDouble
  }

  def squaredError(actual:Double, pred : Double) : Double = {
    return Math.pow( (pred - actual), 2.0)
  }

  def absError(actual:Double, pred : Double) : Double = {
    return Math.abs( (pred - actual))
  }

  def squaredLogError(actual:Double, pred : Double) : Double = {
    return Math.pow( (Math.log(pred +1) - Math.log(actual +1)), 2.0)
  }
/*
def squared_error(actual, pred):
    return (pred - actual)**2

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2
 */
}
