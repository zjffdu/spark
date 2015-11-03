package org.apache.spark.sql.execution.datasources.remote

import java.io.{InputStreamReader, BufferedReader}

//import org.apache.http.client.methods.HttpGet
//import org.apache.http.impl.client.{HttpClientBuilder}
//
///**
// * Created by jzhang on 11/2/15.
// */
//object RestUtils {
//
//  def get(url: String) = {
//    val client = HttpClientBuilder.create().build();
//    val request = new HttpGet(url);
//    val response = client.execute(request);
//    scala.io.Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
//  }
//
//  def main(args:Array[String]): Unit = {
//    println(get("http://baidu.com"))
//  }
//}
