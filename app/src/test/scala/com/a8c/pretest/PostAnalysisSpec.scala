package com.a8c.pretest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PostAnalysisSpec extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _ 
  var countDf: DataFrame = _ 

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PostAnalysisSpec")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val spark2 = spark
    import spark2.implicits._   

    val data = Seq(
      (1, "A"),
      (1, "B"),
      (1, "C"),
      (2, "A"),
      (2, "B"),
      (3, "C")
    )
    countDf = spark.createDataFrame(data).toDF("id", "letter")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("getRowCountDistPerId function should return a DataFrame") {
    val result = PostAnalysis.getRowCountDistPerId(countDf, "id", "label")
    assert(result.isInstanceOf[DataFrame])
  }

  test("getRowCountDistPerId function should return all summary stats") {
    val result = PostAnalysis.getRowCountDistPerId(countDf, "id", "label")
    val resultData = result.collect()

    assert(result.isInstanceOf[DataFrame])

    // expect result to have 5 rows
    assert(result.count() == 5)

    // row labels 
    assert(resultData(0)(0) == "count")
    assert(resultData(1)(0) == "mean")
    assert(resultData(2)(0) == "stddev")
    assert(resultData(3)(0) == "min")
    assert(resultData(4)(0) == "max") 
  }

  test("getRowCountDistPerId function should return correct result") {
    val result = PostAnalysis.getRowCountDistPerId(countDf, "id", "count")
    val resultData = result.collect()

    // grouped dataFrame should have values: 
    // id , count
    // 1  , 3
    // 2  , 2
    // 3  , 1 

    // summary statistics of field 'count' should then be : 
    assert(resultData(0)(1) == "3") // count
    assert(resultData(1)(1) == "2.0") // mean
    assert(resultData(2)(1) == "1.0") // stddev
    assert(resultData(3)(1) == "1") // min
    assert(resultData(4)(1) == "3") // max
  }

  test("getMeanPostsPerBlogPerYear should return a dataframe") {
    val data = Seq(
      (1, 2020),
      (1, 2020),
      (1, 2021),
      (2, 2020),
      (2, 2021),
      (3, 2022)
    )
    val df = spark.createDataFrame(data).toDF("blog_id", "year")
    val result = PostAnalysis.getMeanPostsPerBlogPerYear(df)
    assert(result.isInstanceOf[DataFrame]) 
  }

  test("getMeanPostsPerBlogPerYear gives expected result") {
    val data = Seq(
      (1, 2020),
      (1, 2020),
      (1, 2021),
      (2, 2021),
      (2, 2021),
      (3, 2022)
    )
    val df = spark.createDataFrame(data).toDF("blog_id", "year")

    // count of posts per blog per year will be :
    // blog_id, year, count
    // 1      , 2020, 2
    // 1      , 2021, 1
    // 2      , 2021, 2
    // 3      , 2022, 1

    // Then the mean per year will be : 
    // descending year: 
    // year, mean(count)
    // 2022, 1.0
    // 2021, 1.5
    // 2020, 2.0

    val expectedData = Seq(
      (2022, 1.0),
      (2021, 1.5),
      (2020, 2.0)
    )
    val expectedResult = spark.createDataFrame(expectedData).toDF("year", "mean_posts_per_blog_per_year")
    val result = PostAnalysis.getMeanPostsPerBlogPerYear(df)
    assert(result.collect().sameElements(expectedResult.collect()))
  }

  test("cleanAndPreConditions should return a dataframe") {
    val data = Seq(
      (1L, 1L, "title1", "body1", "10"),
      (2L, 1L, "title1", "body1", "30")
    )
    val goodDf = spark.createDataFrame(data).toDF("blog_id", "post_id", "title", "body", "comment_count")
    val result = PostAnalysis.cleanAndPreConditions(goodDf)
    assert(result.isInstanceOf[DataFrame])
  }

  test("cleanAndPreConditions should return the same data but with the comment_count column cast to Long") {
    val data = Seq(
      (1L, 1L, "title1", "body1", "10"),
      (2L, 1L, "title1", "body1", "30")
    )
    val goodDf = spark.createDataFrame(data).toDF("blog_id", "post_id", "title", "body", "comment_count")

    val result = PostAnalysis.cleanAndPreConditions(goodDf)
    val expectedData = Seq(
      (1L, 1L, "title1", "body1", 10L),
      (2L, 1L, "title1", "body1", 30L)
    )
    val expectedResult = spark.createDataFrame(expectedData).toDF("blog_id", "post_id", "title", "body", "comment_count")

    assert(result.collect().sameElements(expectedResult.collect()))
  }

  test("cleanAndPreConditions should throw an exception when there are duplicate rows") {
    val data = Seq(
      (1L, 1L, "title1", "body1", 10),
      (1L, 1L, "title2", "body2", 20),
      (2L, 2L, "title3", "body3", 30)
    )
    val badDf = spark.createDataFrame(data).toDF("blog_id", "post_id", "title", "body", "comment_count")

    intercept[IllegalStateException] {
      PostAnalysis.cleanAndPreConditions(badDf)
    }
  }

  test("timeExecution should measure the execution time of a block of code") {
    val codeBlock = {
      Thread.sleep(500) // simulate some code that takes time to execute
      10 + 20
    }
    val (result, elapsedTime) = PostAnalysis.timeExecution(codeBlock)
    assert(result == 30)
    assert(elapsedTime > 0)
  }

  test("getRatioOfNonAuthorLikesToTotalLikes should return the ratio of non-author likes to total likes") {
    // Create sample data
    val data = Seq(
      (1L, 1L, "en", "url1", "2019-01-01 00:00:00", "title1", "content1", 1L, "author1", Seq(2L, 3L), 2, Seq(4L, 5L), 2),
      (2L, 2L, "en", "url2", "2019-01-02 00:00:00", "title2", "content2", 2L, "author2", Seq(3L, 4L), 2, Seq(5L, 6L), 2)
    )
    val df = spark.createDataFrame(data).toDF(
      "blog_id", "post_id", "lang", "url", "date_gmt", "title", "content", "author_id", "author_login", "liker_ids",
      "like_count", "commenter_ids", "comment_count"
    )

    // Execute function to test
    val ratio = PostAnalysis.getRatioOfNonAuthorLikesToTotalLikes(df)

    // Expected result is (3 non-author likes) / (4 total likes)
    val expectedRatio = 0.75

    // Verify the result is as expected
    assert(ratio == expectedRatio)
  }
}