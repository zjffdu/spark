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
//import org.apache.hadoop.io.compress.{GzipCodec, SnappyCodec}
//import org.apache.spark.sql.types.DataTypes
//import org.apache.spark.sql.{SaveMode, SQLContext}
//import org.apache.spark.{SparkContext, SparkConf}
////import test.MyUDF
//
//object SparkSQLExample {
//
//  def main(args:Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    //    val loader = new DatasetLoader(sqlContext)
//    //    loader.list("libsvm")
//    //    val df =loader.get("libsvm","a1a")
//    //    df.show()
//    //    df.count()
//    //    df.select("label").show()
////    val df = sqlContext.read.text("file:///Users/jzhang/Temp/hive.log")
////
////    df.write.mode(SaveMode.Overwrite)
//////      .option("compression.codec","snappy")
//////      .compress(classOf[GzipCodec])
////      .text("file:///Users/jzhang/Temp/text_data2")
////
////    val df2 =sqlContext.read.text("file:///Users/jzhang/Temp/text_data2")
//////    df.show()
//
////    import org.apache.spark.sql.functions._
////    val df = sqlContext.createDataFrame(Seq((1,"jeff"))).toDF("id", "name")
////    df.show()
////    sqlContext.udf.register("myudf", new MyUDF(), DataTypes.StringType)
////    df.withColumn("name", expr("myudf(name)")).show()
//  }
//}
