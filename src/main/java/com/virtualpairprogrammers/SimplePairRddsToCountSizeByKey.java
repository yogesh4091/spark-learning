package com.virtualpairprogrammers;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SimplePairRddsToCountSizeByKey {

  public static void main(String[] args) {

    Logger logger = Logger.getLogger("org.apache");
    logger.setLevel(Level.WARN);

    List<String> logMessages = new ArrayList<>();
    logMessages.add("WARN: Line 1");
    logMessages.add("FATAL: Line 2");
    logMessages.add("ERROR: Line 3");
    logMessages.add("INFO: Line 4");
    logMessages.add("WARN: Line 5");
    logMessages.add("FATAL: Line 6");
    logMessages.add("ERROR: Line 7");
    SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

      // Efficient way
      javaSparkContext
          .parallelize(logMessages)
          .mapToPair(message -> new Tuple2<>(message.split(":")[0], 1L))
          .reduceByKey(Long::sum)
          .collect()
          .forEach(logger::warn);

      // GroupBy key DON'T USE THIS METHOD UNLESS ABSOLUTELY NECESSARY.
      javaSparkContext
          .parallelize(logMessages)
          .mapToPair(message -> new Tuple2<>(message.split(":")[0], message.split(":")[1]))
          .groupByKey()
          .collect()
          .forEach(tuple -> logger.warn(tuple._1 + "," + Iterables.size(tuple._2)));

      javaSparkContext
          .parallelize(logMessages)
          .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
          .filter(value -> value.length() > 1)
          .collect()
          .forEach(logger::warn);
    }
  }
}
