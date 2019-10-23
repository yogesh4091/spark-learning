package com.virtualpairprogrammers;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MostFrequentlyUsedWordsInFile {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("MFU Words").setMaster("local[*]");

    try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

      JavaRDD<String> initialRdd =
          javaSparkContext.textFile("src/main/resources/subtitles/input-spring.txt");

      JavaRDD<String> lettersOnlyRDD =
          initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

      JavaRDD<String> removedBlankLines =
          lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);

      JavaRDD<String> justWords =
          removedBlankLines.flatMap(value -> Arrays.asList(value.trim().split(" ")).iterator());

      JavaRDD<String> interestingWords = justWords.filter(Util::isNotBoring);

      JavaPairRDD<String, Long> wordCountTupleRdd =
          interestingWords.mapToPair(value -> new Tuple2<>(value, 1L)).reduceByKey(Long::sum);

      JavaPairRDD<Long, String> switchedRdd =
          wordCountTupleRdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

      switchedRdd.sortByKey(false).take(10).forEach(System.out::println);

      //      initialRdd
      //          .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
      //          .filter(Util::isNotBoring)
      //          .mapToPair(value -> new Tuple2<>(value, 1L))
      //          .reduceByKey(Long::sum)
      //          .collect()
      //          .forEach(System.out::println);
    }
  }
}
