package com.a8c.pretest

import org.apache.spark.sql.SparkSession

object CreateSamplePosts {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: CreateSamplePosts <file> 
        |  <inpath> inpath to the text file (jsonl.gz) 
        |  <outpath> outpath to the text file (jsonl)
           <n> num random records
        """.stripMargin)
      System.exit(1)
    }
    var spark = SparkSession.builder().appName("Sample Posts").master("local[*]").getOrCreate()

    // Set log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json(args(0))

    // write sample data 
    val fraction = args(2).toFloat / df.count()
    val withReplacement = false
    val seed = 42 // Random seed for reproducibility
    val sampleDf = df.sample(withReplacement, fraction, seed)
    sampleDf.write.json(args(1))
  }

}