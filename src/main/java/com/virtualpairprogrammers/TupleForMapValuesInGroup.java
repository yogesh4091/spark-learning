package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TupleForMapValuesInGroup {
  private static Logger logger = Logger.getLogger("org.apache");

  public static void main(String[] args) {
    logger.setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
      List<Integer> integers = new ArrayList<>();
      integers.add(11);
      integers.add(9);
      integers.add(4);
      integers.add(13);
      JavaRDD<Integer> integerRdd = javaSparkContext.parallelize(integers);

      // Finding Square route of RDD of Integers and keeping key value in Tuple2.
      JavaRDD<Tuple2<Integer, Double>> squareRootTupleRdd =
          integerRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
      logger.warn("Key Value Using Tuple");
      squareRootTupleRdd.collect().forEach(logger::warn);
    }
  }
}
