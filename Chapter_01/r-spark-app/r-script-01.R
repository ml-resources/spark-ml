Sys.setenv(SPARK_HOME = "/PATH/spark-1.6.0-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
sc <- sparkR.init(master = "local", sparkPackages="com.databricks:spark-csv_2.10:1.3.0")
sqlContext <- sparkRSQL.init(sc)

user.purchase.history <- "/PATH/spark-ml/Chapter_01/r-spark-app/data/UserPurchaseHistory.csv"
data <- read.df(sqlContext, user.purchase.history, "com.databricks.spark.csv", header="false")
head(data)
count(data)

parseFields <- function(record) {
  Sys.setlocale("LC_ALL", "C") # necessary for strsplit() to work correctly
  parts <- strsplit(as.character(record), ",")
  list(name=parts[1], product=parts[2], price=parts[3])
}

parsedRDD <- SparkR:::lapply(data, parseFields)
cache(parsedRDD)
numPurchases <- count(parsedRDD)

sprintf("Number of Purchases : %d", numPurchases)
getName <- function(record){
  record[1]
}

nameRDD <- SparkR:::lapply(parsedRDD, getName)
nameRDD = collect(nameRDD)
head(nameRDD)

uniqueUsers <- unique(nameRDD)
head(uniqueUsers)

prices <- SparkR:::lapply(parsedRDD, function(x) { x$price })
take(prices, 5)

prices <- SparkR:::lapply(prices, function(x) { list(1, as.numeric(x)) })
take(prices, 5)
totalRevenue <- SparkR:::reduceByKey(prices, "+", 1L)
totalRevenueNum <- first(totalRevenue)[2][1]
sprintf("Total Revenue : %.2f", totalRevenueNum)

products <- SparkR:::lapply(parsedRDD, function(x) { list( toString(x$product[1]), 1) })
take(products, 5)
productCount <- SparkR:::reduceByKey(products, "+", 2L)
productsCountAsKey <- SparkR:::lapply(productCount, function(x) { list( as.integer(x[2][1]), x[1][1])})

productCount <- count(productsCountAsKey)
mostPopular <- toString(collect(productsCountAsKey)[[productCount]][[2]])
sprintf("Most Popular Product : %s", mostPopular)
