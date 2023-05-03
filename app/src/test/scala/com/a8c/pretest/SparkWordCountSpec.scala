package com.a8c.pretest

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class SparkWordCountSpec extends AnyFlatSpec with BeforeAndAfterEach with Matchers with SparkContextSetup {

  import SparkWordCount._

  "Spark" should "not be stopped" in withSparkContext { (sc) =>
    sc.isStopped should be (false)
  }

  "Counting all words" should "return all three words" in withSparkContext { (sc) =>
    val input: RDD[String] = sc.parallelize(List(
      "Hello world",
      "Goodbye, world."
    ))

    getWordCounts(input, 1).collect() should be (Array(
      ("world",   2),
      ("goodbye", 1),
      ("hello",   1)
    ))
  }

  "Counting multiple instances of words" should "return subset of words" in withSparkContext { (sc) =>
    val input: RDD[String] = sc.parallelize(List(
      "hello world",
      "goodbye world"
    ))

    getWordCounts(input, 2).collect() should be (Array(
      ("world",   2)
    ))
  }
}

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkContext) => Any) {
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("Spark Test")
    )

    try {
      sc.setLogLevel("ERROR")
      testMethod(sc)
    }
    finally {
      sc.stop()
    }
  }
}
