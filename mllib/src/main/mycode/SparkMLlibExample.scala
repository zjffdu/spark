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
//import org.apache.spark.sql.{SaveMode, SQLContext}
//import org.apache.spark.{SparkContext, SparkConf}
//
//
//object SparkMLlibExample {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    //    val loader = new DatasetLoader(sqlContext)
//    //    loader.list("libsvm")
//    //    val df =loader.get("libsvm","a1a")
//    //    df.show()
//    //    df.count()
//    //    df.select("label").show()
//    val df = sqlContext.read.format("libsvm").
//      load("file:///Users/jzhang/github/spark/data/mllib/sample_libsvm_data.txt")
//    df.printSchema()
//    df.show()
//
//
////    df.write.format("libsvm").mode(SaveMode.Overwrite).save("file:///Users/jzhang/Temp/libsvm")
////    df.write.format("json").mode(SaveMode.Overwrite).save("file:///Users/jzhang/Temp/json")
////    val df2 = sqlContext.createDataFrame(Seq((1,"name"),(2,"jeff"))).toDF("label", "features")
////    df2.write.format("libsvm").mode(SaveMode.Overwrite).save("file:///Users/jzhang/Temp/libsvm2")
//////    df.write.format("text").mode(SaveMode.Overwrite).save("file:///Users/jzhang/Temp/text")
//
//
//
//  }
//}
