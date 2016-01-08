Sys.setenv(SPARK_HOME = "/home/ubuntu/work/spark-1.5.2-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
sc <- sparkR.init(master = "local", sparkPackages="com.databricks:spark-csv_2.10:1.3.0")
sqlContext <- sparkRSQL.init(sc)

user.purchase.history <- "/home/ubuntu/work/rajdeepd-spark-ml/spark-ml/Chapter_01/r-spark-app/data/UserPurchaseHistory.csv"
data <- read.df(sqlContext, user.purchase.history, "com.databricks.spark.csv", header="false")
head(data)
count(data)

parseFields <- function(record) {
  Sys.setlocale("LC_ALL", "C") # necessary for strsplit() to work correctly
  parts <- strsplit(as.character(record), ",")[[1]]
  list(name=parts[1], product=parts[2], price=parts[3])
}


parsedRDD <- SparkR:::lapply(data, parseFields)
cache(parsedRDD)
numPurchases <- count(parsedRDD)
sprintf("Number of Purchases : %d", numPurchases)
getName <- function(record){
  record[1]
}

#nameRDD <- SparkR:::lapply(parsedRDD, function(x) { x$name })
nameRDD <- SparkR:::lapply(parsedRDD, getName)
head(nameRDD)

#uniqueUsers <- distinct(nameRDD)
first(uniqueUsers)

