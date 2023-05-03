package com.a8c.pretest

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: SparkWordCount <file> <threshold>
        |  <file> path to the text file to counts words from
        |  <threshold> minimum occurrences required for report
        """.stripMargin)
      System.exit(1)
    }

    // create Spark context with Spark configuration
    val sc = new SparkContext(
      new SparkConf().setAppName("Spark Word Count").setMaster("local[*]")
    )
    sc.setLogLevel("ERROR")

    // get data and threshold
    val lines: RDD[String] = sc.textFile(args(0))
    val threshold: Int = args(1).toInt

    // dump results to stdout
    printf(s"""
      |Words in '%s' that appear %d time(s) or more:
      |  %s
      |
      |""".stripMargin,
      args(0),
      args(1).toInt,
      getWordCounts(lines, threshold)
        .collect()
        .mkString("\n  ")
    )
  }

  def getWordCounts(lines: RDD[String], threshold: Int): RDD[(String, Int)] = {
    lines
      .flatMap(_.split(" "))            // tokenize into words
      .map{x: String =>
        x
          .toLowerCase                  // normalize to lowercase
          .stripSuffix(",")             // strip ','
          .stripSuffix(".")             // strip '.'
      }
      .filter(_ != "")                  // filter empty words
      .map((_, 1))                      // map each word to '1'
      .reduceByKey(_ + _)               // reduce and sum '1's to count words
      .filter(_._2 >= threshold)        // filter to above threshold occurrences
      .sortBy(_._2, false)              // sort most to least occurrences
  }

}
