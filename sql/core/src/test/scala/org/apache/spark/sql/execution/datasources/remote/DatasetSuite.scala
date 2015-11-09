package org.apache.spark.sql.execution.datasources.remote

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, DataFrame, AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class DatasetSuite extends QueryTest with SharedSQLContext {

  test("reading text file") {
    val datasetLoader = new DatasetLoader(sqlContext)
//    datasetLoader.list()
  }
//
//  test("SQLContext.read.text() API") {
//    verifyFrame(sqlContext.read.text(testFile))
//  }
//
//  test("writing") {
//    val df = sqlContext.read.text(testFile)
//
//    val tempFile = Utils.createTempDir()
//    tempFile.delete()
//    df.write.text(tempFile.getCanonicalPath)
//    verifyFrame(sqlContext.read.text(tempFile.getCanonicalPath))
//
//    Utils.deleteRecursively(tempFile)
//  }
//
//  test("error handling for invalid schema") {
//    val tempFile = Utils.createTempDir()
//    tempFile.delete()
//
//    val df = sqlContext.range(2)
//    intercept[AnalysisException] {
//      df.write.text(tempFile.getCanonicalPath)
//    }
//
//    intercept[AnalysisException] {
//      sqlContext.range(2).select(df("id"), df("id") + 1).write.text(tempFile.getCanonicalPath)
//    }
//  }
//
//  private def testFile: String = {
//    Thread.currentThread().getContextClassLoader.getResource("text-suite.txt").toString
//  }
//
//  /** Verifies data and schema. */
//  private def verifyFrame(df: DataFrame): Unit = {
//    // schema
//    assert(df.schema == new StructType().add("text", StringType))
//
//    // verify content
//    val data = df.collect()
//    assert(data(0) == Row("This is a test file for the text data source"))
//    assert(data(1) == Row("1+1"))
//    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
//    // scalastyle:off
//    assert(data(2) == Row("数据砖头"))
//    // scalastyle:on
//    assert(data(3) == Row("\"doh\""))
//    assert(data.length == 4)
//  }
}
