//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkContext, SparkConf}
//
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//
//object SparkHiveExample {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    val hiveContext = new HiveContext(sc)
//    import sqlContext.implicits._
//    import sqlContext._
//    import org.apache.spark.sql.types._
//
//    //    val df = sqlContext.read.json("file:///Users/jzhang/Temp/json_data")
//    //    df.registerTempTable("df")
//    //    sqlContext.udf.register("myudf", (x: Int) => x * 2)
//    //    sqlContext.sql("select * from df where id>6 and myudf(value)>10").explain()
//
//    val schema = new StructType().add("id", "integer")
//    hiveContext.tables().show()
//    //    hiveContext.sql(
//    //      s"""CREATE EXTERNAL TABLE normal_orc2(
//    //         |  intField INT,
//    //         |  stringField STRING
//    //         |)
//    //         |STORED AS ORC
//    //         |LOCATION 'file:///Users/jzhang/Temp/empty2'
//    //       """.stripMargin)
//
//
//    try {
//      sqlContext.read.text("file:///Users/jzhang/Temp/empty")
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    try {
//      sqlContext.read.json("file:///Users/jzhang/Temp/empty")
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//
//    try {
//      sqlContext.read.parquet("file:///Users/jzhang/Temp/empty")
//    } catch {
//      case e: AssertionError => e.printStackTrace()
//    }
//
//    try {
//      hiveContext.read.orc("file:///Users/jzhang/Temp/empty")
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//
//
//
//    hiveContext.sql("create temporary table test_json(id int) using json options (path 'file:///Users/jzhang/Temp/empty')")
//    hiveContext.sql("create temporary table test_text using text options (path 'file:///Users/jzhang/Temp/empty')")
//    hiveContext.sql("create temporary table test_parquet(id int) using parquet options (path 'file:///Users/jzhang/Temp/empty')")
//    hiveContext.sql("create temporary table test_orc(id int) using orc options (path 'file:///Users/jzhang/Temp/empty')")
//
//
//    hiveContext.sql("select count(*) from test_json").show()
//    hiveContext.sql("select count(*) from test_text").show()
//    hiveContext.sql("select count(*) from test_parquet").show()
//    hiveContext.sql("select count(*) from test_orc").show()
//  }
//}
