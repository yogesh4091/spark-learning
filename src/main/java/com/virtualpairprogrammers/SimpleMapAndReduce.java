package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleMapAndReduce {

  private static Logger logger = Logger.getLogger("org.apache");

  public static void main(String[] args) {

    List<Integer> doubles = new ArrayList<>();
    doubles.add(11);
    doubles.add(9);
    doubles.add(4);
    doubles.add(13);

    logger.setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext javaSparkContext = null;
    try {
      javaSparkContext = new JavaSparkContext(sparkConf);
      JavaRDD<Integer> integerRdd = javaSparkContext.parallelize(doubles);

      // Finding Sum of Integers
      logger.warn("Sum : " + integerRdd.reduce(Integer::sum));

      // Finding Square route of RDD of Integers.
      JavaRDD<Double> squareRootRdd = integerRdd.map(Math::sqrt);
      logger.warn("Square Route :");
      squareRootRdd.collect().forEach(logger::warn);

      logger.warn("Count Using Method :" + integerRdd.count());

      logger.warn("Count using Map and Reduce :" + integerRdd.map(val1 -> 1).reduce(Integer::sum));

    } finally {
      if (javaSparkContext != null) {
        javaSparkContext.close();
      }
    }
  }
}
