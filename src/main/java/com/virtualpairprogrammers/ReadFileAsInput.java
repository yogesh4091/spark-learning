package com.virtualpairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFileAsInput {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("readFile").setMaster("local[*]");
    try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

      JavaRDD<String> subtitles =
          javaSparkContext.textFile("src/main/resources/subtitles/input.txt");
      subtitles
          //          .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
          .collect()
          .forEach(System.out::println);
    }
  }
}
