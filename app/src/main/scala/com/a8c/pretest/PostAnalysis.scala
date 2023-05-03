package com.a8c.pretest

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream
import java.io.PrintWriter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.HashSet
import scala.collection.immutable.Map
import org.apache.spark.sql.types._

object PostAnalysis {

   def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: PostAnalysis <inputfile> <outputLogFile>
        |  <file> path to the text file (jsonl.gz) to analyze
        |  <outputLogFile> path to the output log file
        """.stripMargin)
      System.exit(1)
    }

    val dbName = "ctrobertwest"
    val tableName = "dataset"

    // ----------------------------------
    
    val spark = SparkSession.builder()
      .appName("Spark Post Analysis")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // Set log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = args(0)
    val outputLogPath = args(1)

    val schema = getDatasetSchema()
    val raw_df = spark.read.schema(schema).json(inputPath)
    val df = cleanAndPreConditions(raw_df)
      .withColumn("year", year(col("date_gmt"))) // adding a year field for partitioning and statistics

    // Test 1 - Pre condition above allows us to use simple count
    val test1_result = getRowCountDistPerId(df, "blog_id", "Posts Per Blog")
    val test1_bonus_per_year = getMeanPostsPerBlogPerYear(df)

    // Test 2
    val test2_result = getRatioOfNonAuthorLikesToTotalLikes(df)
    // Test 3
  
    val dfWrite = transformFieldsBeforeWrite(df)

    val createTblQuery = getCreateTableQuery(tableName)
    val (_, writeElapsedTime) = timeExecution(
      makeTableAndWriteData(spark, createTblQuery, dfWrite, dbName, tableName))
    val sampleQueries = getSampleQueries(tableName)

    val (sampleQueryResults, readElapsedTime) = timeExecution(
      runSampleQueries(spark, sampleQueries))

    // Print Results 
    printResults(inputPath, outputLogPath, test1_result, test1_bonus_per_year, test2_result, createTblQuery, sampleQueryResults,
      writeElapsedTime, readElapsedTime)
  }

  def timeExecution[T](block: => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = block
    val endTime = System.nanoTime()
    val elapsedTime = endTime - startTime
    (result, elapsedTime)
  }

  def getDatasetSchema() : StructType = {
    new StructType()
      .add("blog_id", LongType)
      .add("post_id", LongType)
      .add("lang", StringType)
      .add("url", StringType)
      .add("date_gmt", StringType)
      .add("title", StringType)
      .add("content", StringType)
      .add("author", StringType)
      .add("author_login", StringType)
      .add("author_id", LongType)
      .add("liker_ids", ArrayType(LongType))
      .add("like_count", IntegerType)
      .add("commenter_ids", ArrayType(LongType))
      .add("comment_count", StringType)
  }

  def cleanAndPreConditions(df: DataFrame): DataFrame = {
    // Data Pre-condition validation :
    if (df.count() != df.dropDuplicates(Seq("blog_id", "post_id")).count()){
       val errorMessage = s"Dataset doesn't meet the expectation: Found duplicate rows based on 'blog_id' and 'post_id'."
       throw new IllegalStateException(errorMessage)
    }

    // for some reason comment_count was coming out with all nulls if I tried to make it an Int in the schema
    df.withColumn("comment_count", df("comment_count").cast(IntegerType))
  }

  def getRowCountDistPerId(df: DataFrame, id: String, label: String): DataFrame = {
    val grouped = df.groupBy(id).agg(count("*").alias(label))
    grouped.describe(label)
  }

  def getPostsPerBlogDist(df: DataFrame): DataFrame = {
    getRowCountDistPerId(df, "blog_id", "Posts Per Blog")
  }

  def getMeanPostsPerBlogPerYear(df: DataFrame): DataFrame = {
    val postsPerBlogPerYear = df.groupBy("blog_id", "year").agg(count("*").alias("count"))
    postsPerBlogPerYear.groupBy("year").agg(mean("count").alias("mean_posts_per_blog_per_year")).orderBy(desc("year")) 
  }

  def getRatioOfNonAuthorLikesToTotalLikes(df: DataFrame): Double = {
    val author_ids = HashSet() ++ df.select("author_id").distinct().collect().map(_.getLong(0))

    val df_w_liker_ids = df.filter(col("liker_ids").isNotNull)
    val liker_ids = df_w_liker_ids.select("liker_ids").rdd.flatMap(row => row.getAs[Seq[Long]](0).toList)

    val nonAuthorLikeCount = liker_ids.filter(id => !author_ids.contains(id)).count()
    val likeCount = liker_ids.count()
    nonAuthorLikeCount.toFloat / likeCount
  }

  def getCreateTableQuery(tableName: String): String = {
      s"""
        CREATE TABLE $tableName (
          blog_id BIGINT,
          post_id BIGINT,
          lang STRING,
          url STRING,
          year INT, 
          date_gmt DATE,
          title STRING,
          content STRING,
          author STRING,
          author_login STRING,
          author_id BIGINT,
          liker_ids STRING,
          like_count INT,
          commenter_ids STRING,
          comment_count INT
        )
        PARTITIONED BY (lang, year)
        STORED AS PARQUET
      """
  }

  def transformFieldsBeforeWrite(df: DataFrame) : DataFrame = {
    val delimiter = ","
    df
      .withColumn("liker_ids", array_join(col("liker_ids"), delimiter))
      .withColumn("commenter_ids", array_join(col("commenter_ids"), delimiter))
      .withColumn("date_gmt", to_timestamp(col("date_gmt"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

  }

  def makeTableAndWriteData(spark: SparkSession, createTblQuery: String, df: DataFrame, 
                dbName: String, tableName: String): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    spark.sql(s"USE $dbName")
    spark.sql(createTblQuery)

    df.write
      .partitionBy("lang", "year")
      .mode(SaveMode.Append)
      .format("hive")
      .saveAsTable(s"$dbName.$tableName")
  }
  
  def getSampleQueries(tableName: String): Array[String] = {
    Array(
        s"""SELECT count(*) AS posts, lang
          FROM $tableName
          WHERE year=2010 AND 
            date_gmt BETWEEN '2010-01-01 00:00:00' AND '2010-12-31 23:59:59'
          GROUP BY lang
        """,

        s"""SELECT sum(comment_count) AS comments
        FROM $tableName
        WHERE
          lang = 'en' AND
          year in (2011, 2012) AND
          date_gmt BETWEEN '2012-01-01' AND '2012-01-31'
        """,

        s"""
        SELECT sum(like_count) / sum(comment_count) AS ratio
        FROM $tableName
        """
      )
  }

  def runSampleQueries(spark: SparkSession, queries: Array[String]): Map[String, DataFrame] = {
    var resultMap = Map[String, DataFrame]()
    for (query <- queries) {
      val result = spark.sql(query)
      resultMap = resultMap + (query -> result)
    }
    resultMap
  }


  def printMarkDownTable(df: DataFrame, pw: PrintWriter, numRows: Int = 10, showTail: Boolean = false): Unit = {
    pw.println("|" + df.columns.mkString("|") + "|")
    pw.println("|" + df.columns.map(_ => "---").mkString("|") + "|")
    df.limit(numRows).collect().foreach { row =>
        pw.println("|" + row.toSeq.map(_.toString).mkString("|") + "|")
    }
  }

  def getPrintWriter(outputLogFile: String) = {
    val outputFile = new File(outputLogFile)
    val fileOutputStream = new FileOutputStream(outputFile)
    val printStream = new PrintStream(fileOutputStream)
    val printWriter = new PrintWriter(printStream)
    (printWriter, fileOutputStream)
  }

  def closePrintWriter(printWriter: PrintWriter, fileOutputStream: FileOutputStream): Unit = {
    printWriter.flush()
    printWriter.close()
    fileOutputStream.close()
  }

  def printResults(inputFile: String, outputLogFile: String, test1_result: DataFrame, test1_bonus_per_year: DataFrame,
                   test2_result: Double, createTblQuery: String, queryResults: Map[String, DataFrame], 
                   writeElapsedTime: Double, readElapsedTime: Double): Unit = {

    val (pw, fileOutputStream) = getPrintWriter(outputLogFile)

    pw.println("Blog Post Analysis")
    pw.println("=========================================")
    pw.println("\n")
    pw.println("Input File used for this run: ")
    pw.println("--------------------------")
    pw.println(inputFile)
    pw.println("\n")
    pw.println("Posts Per Blog")
    pw.println("--------------------------")
    pw.println("\n") 
    printMarkDownTable(test1_result, pw)
    pw.println(" ")
    pw.println("Last 10 Years Avg posts per blog per year: ")
    printMarkDownTable(test1_bonus_per_year, pw)
    pw.println("\n")
    pw.println("Who Likes Posts?")
    pw.println("----------------------------")
    pw.printf("Percentage of likes from non-authors: %.2f%%" , test2_result * 100.0)
    pw.println("\n")

    pw.println("Writing a Table")
    pw.println("----------------------------")
    pw.println("Create Table Query : \n")
    pw.printf(""" %s """, createTblQuery)
    pw.println("\n")
    pw.println("Function used to write data to disk:")
    pw.println("""
        def makeTableAndWriteData(spark: SparkSession, createTblQuery: String, df: DataFrame, 
                dbName: String, tableName: String): Unit = {
          spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
          spark.sql(s"USE $dbName")
          spark.sql(createTblQuery)

          df.write
            .partitionBy("lang", "year")
            .mode(SaveMode.Append)
            .format("hive")
            .saveAsTable(s"$dbName.$tableName")
        }""")
    pw.println("For further details see the file `PostAnalysis.scala`")
    pw.println("\n")
    pw.println(s"Time to write table: ${writeElapsedTime / 1e9} seconds")
    pw.println(s"Time to read table: ${readElapsedTime / 1e9} seconds")

    pw.println("Sample user queries and results: ")
    pw.println("```")

    queryResults.foreach { case (query, df) =>
      pw.println(query)
      pw.println("#----------------------------#")
      pw.println("")
      pw.println(df.collect().mkString("\n"))
      pw.println("")
      pw.println("#----------------------------#")
    }
    pw.println("```")
    pw.println("\n")
    pw.println("<p>As per the sample queries provided I have chosen to partition the dataset on both <code>lang</code> and <code>year</code>. The field <code>year</code> is an integer imputed from the <code>date_gmt</code> field.</p>")
    pw.println("<p>Essentially what this allows, is to partition the dataset by <code>lang</code> and <code>year</code>, two fields that the user will likely use in where clauses or group bys. By using <code>year</code> instead of <code>date_gmt</code> we are able to drastically reduce the number of partitions to 10 (2010-2019) instead of 3650 (2010-2019) possibly per each <code>lang</code>. This will allow for efficient queries on both of these columns.</p>")
    pw.println("<p>The down side is you now require a more sophisticated user query to get the data to ensure <code>year</code> is included before <code>date_gmt</code> in the where clause. On a more powerful system you could just partition by <code>lang</code> and <code>date_gmt</code> and not worry about <code>year</code>, but this is not practical when running locally.</p>")
    pw.println("<p>In general the use of two fields in our partition will slow down the write of the data, but will speed up queries on groupings of <code>lang</code> and <code>year</code></p>.")
    pw.println("<p>An assumption I have made is that a user would be more likely to query the dataset by <code>lang</code> before <code>date_gmt</code>.</p>")
    closePrintWriter(pw, fileOutputStream)
  }
}