package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class JoinsInSpark {
  private static Logger logger = Logger.getLogger("org.apache");

  public static void main(String[] args) {
    logger.setLevel(Level.WARN);
    SparkConf sparkConf = new SparkConf().setAppName("InnerJoin").setMaster("local[*]");
    try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

      List<Tuple2<Integer, Long>> userVisits = new ArrayList<>();
      userVisits.add(new Tuple2<>(1, 5L));
      userVisits.add(new Tuple2<>(2, 3L));
      userVisits.add(new Tuple2<>(3, 51L));
      JavaPairRDD<Integer, Long> userVisitRdd = javaSparkContext.parallelizePairs(userVisits);
      System.out.println(" User Visits RDD : ");
      userVisitRdd.collect().forEach(System.out::println);

      List<Tuple2<Integer, String>> userNames = new ArrayList<>();
      userNames.add(new Tuple2<>(1, "Yogesh"));
      userNames.add(new Tuple2<>(2, "Shreyas"));
      userNames.add(new Tuple2<>(4, "Abhi"));
      JavaPairRDD<Integer, String> userNamesRdd = javaSparkContext.parallelizePairs(userNames);
      System.out.println(" User Names RDD : ");
      userNamesRdd.collect().forEach(System.out::println);

      System.out.println("Inner Join : ");
      JavaPairRDD<Integer, Tuple2<Long, String>> innerJoinRdd = userVisitRdd.join(userNamesRdd);
      innerJoinRdd
          .collect()
          .forEach(
              row ->
                  System.out.println(
                      "User Id : " + row._1 + "  Name : " + row._2._2 + " Visits : " + row._2._1));

      System.out.println("Left Outer Join : ");

      JavaPairRDD<Integer, Tuple2<Long, Optional<String>>> leftOuterJoinRdd =
          userVisitRdd.leftOuterJoin(userNamesRdd);
      leftOuterJoinRdd
          .collect()
          .forEach(
              row ->
                  System.out.println(
                      "User Id : "
                          + row._1
                          + "  Name : "
                          + row._2._2.orElse("NA")
                          + " Visits : "
                          + row._2._1));

      System.out.println("Right Outer Join : ");

      JavaPairRDD<Integer, Tuple2<Optional<Long>, String>> rightOuterJoinRdd =
          userVisitRdd.rightOuterJoin(userNamesRdd);
      rightOuterJoinRdd
          .collect()
          .forEach(
              row ->
                  System.out.println(
                      "User Id : "
                          + row._1
                          + "  Name : "
                          + row._2._2
                          + " Visits : "
                          + row._2._1.orElse(0L)));

      System.out.println("Cartesian Join");
      JavaPairRDD<Tuple2<Integer, Long>, Tuple2<Integer, String>> crossJoinRdd =
          userVisitRdd.cartesian(userNamesRdd);
      crossJoinRdd.collect().forEach(System.out::println);
    }
  }
}
