import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A simple Spark app in Java (Wordcount example)
 */
public class JavaApp {

    public static void main(String[] args) {
        JavaSparkContext sc = null;

        try {
//           ####### create spark context by starting the cluster locally using 2 CPU cores #######
            sc = new JavaSparkContext("local[2]", "First Spark App");

//           ####### we take the raw data in CSV format and convert it into a set of records of the form (user, product, price),
//           (without lambda for < jdk 1.8) #######

//            JavaRDD<String[]> data = sc.textFile("data/UserPurchaseHistory.csv")
//                    .map(new Function<String, String[]>() {
//                        @Override
//                        public String[] call(String s) throws Exception {
//                            return s.split(",");
//                        }
//                    });

//           ####### with lambda #######
            JavaRDD<String[]> data = sc.textFile("data/UserPurchaseHistory.csv").map(s -> s.split(","));

//           ####### let's count the number of purchases #######
            long numPurchases = data.count();

            System.out.println("NumberPurchases : " + numPurchases);

//           ####### let's count how many unique users made purchases (without lambda for < jdk 1.8) #######
//            long uniqueUsers = data.map(new Function<String[], String>() {
//                @Override
//                public String call(String[] strings) throws Exception {
//                    return strings[0];
//                }
//            }).distinct().count();

//           ####### with lambda #######
            long uniqueUsers = data.map(strings -> strings[0]).distinct().count();

            System.out.println("Unique users : " + uniqueUsers);

//          ####### let's sum up our total revenue (without lambda for < jdk 1.8) #######
//            Double totalRevenue = data.map(new Function<String[], Double>() {
//                @Override
//                public Double call(String[] strings) throws Exception {
//                    return Double.parseDouble(strings[2]);
//                }
//            }).reduce(new Function2<Double, Double, Double>() {
//                private static final long serialVersionUID = 1L;
//
//                public Double call(Double v1, Double v2) throws Exception {
//                    return new Double(v1.doubleValue() + v2.doubleValue());
//                }
//            });

//           ####### with lambda #######
            Double totalRevenue = data.map(strings -> Double.parseDouble(strings[2])).reduce((Double v1, Double v2) -> new Double(v1.doubleValue() + v2.doubleValue()));

            System.out.println("Total revenue : " + totalRevenue);

//          ####### let's find our most popular product (without lambda for < jdk 1.8) #######
//            List<Tuple2<String, Integer>> pairs = data.mapToPair(new PairFunction<String[], String, Integer>() {
//                @Override
//                public Tuple2<String, Integer> call(String[] strings) throws Exception {
//                    return new Tuple2(strings[1], 1);
//                }
//            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer integer, Integer integer2) throws Exception {
//                    return integer + integer2;
//                }
//            }).collect();

//           ####### with lambda #######
            List<Tuple2<String, Integer>> pairs = data.mapToPair(strings -> new Tuple2<String, Integer>(strings[1], 1)).reduceByKey((Integer i1, Integer i2) -> i1 + i2).collect();

            Map<String, Integer> sortedData = new HashMap<>();
            Iterator it = pairs.iterator();
            while (it.hasNext()) {
                Tuple2<String, Integer> o = (Tuple2<String, Integer>) it.next();
                sortedData.put(o._1, o._2);
            }
            List<String> sorted = sortedData.entrySet()
                    .stream()
                    .sorted(Comparator.comparing((Map.Entry<String, Integer> entry) -> entry.getValue()).reversed())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            System.out.println("Most popular products sorted : " + sorted);


            String mostPopular = sorted.get(0);
            int purchases = sortedData.get(mostPopular);
            System.out.println("Most popular product is : " + mostPopular + ", with number of purchases : " + purchases);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }


    }
}
