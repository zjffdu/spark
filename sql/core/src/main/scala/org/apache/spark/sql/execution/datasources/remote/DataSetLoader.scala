package org.apache.spark.sql.execution.datasources.remote

import java.io.IOException

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.collection.mutable
import scala.collection.immutable.Map
import org.apache.spark.Logging

case class RepositoryMetadata(id: String, desc: String, lastUpdated:String,
                              datasets: Map[String, String])

case class DatasetMetadata(id: String, urls: Seq[String], format: String, schemaStr:String,
                           version:String, lastUpdated:String, desc:String) {
  def toOptions():Map[String, String] = {
    Map("remote.id" -> id,
      "remote.urls" -> urls.mkString(","),
      "remote.format" -> format,
      "remote.schema" -> schemaStr,
      "remote.version" -> version,
      "remote.lastUpdated" -> lastUpdated,
      "remote.desc" -> desc)
  }

  val schema: Option[StructType] = {
    if (schemaStr.trim.isEmpty) {
      None
    } else {
      val _schema = new StructType()
      schemaStr.split(",").foreach(e => {
        val tokens = e.split(":")
        _schema.add(tokens(0), tokens(1))
      })
      Some(_schema)
    }
  }
}

class DatasetLoader(sqlContext: SQLContext) extends Logging {

  val repositories: scala.collection.mutable.Map[String, RepositoryMetadata] =
    new mutable.HashMap[String, RepositoryMetadata]()


  def register(repoId: String, url: String): Unit = {
    val repoMetadata : RepositoryMetadata = new RepositoryMetadata(repoId, "desc", "" ,
      Map("a1a" -> "http://dummy", "a2a" -> "http://dummy2"))
    this.repositories += repoId -> repoMetadata
  }

  // register the built-in repository
  register("libsvm", "http://libsvm")
  register("uci", "http://uci")


  def list(repoId: String): Seq[DatasetMetadata] = {
    repositories.get(repoId) match {
      case Some(url) =>
        Seq(DatasetMetadata("a1a", Seq("http://download1", "http://download2"), "libsvm", "",
          "v1", "",""))
      case None => throw new IOException(s"No such repository, repoId={repoId}")
    }
  }

  def list(): Seq[DatasetMetadata] = {
    list("repo1")
  }

  private def getMetadata(repoId: String, datasetId: String) : DatasetMetadata = {
    repositories.get(repoId) match {
      case Some(repoMetadata) => repoMetadata.datasets.get(datasetId) match {
        case Some(datasetURL) =>
          // try this url to fetch dataset metadata
          DatasetMetadata(datasetId, Seq("http://localhost:18088/mysite/a1a"),
            "libsvm", "", "v1", "", "desc")
//          DatasetMetadata(datasetId, Seq("https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv"),
//            "com.databricks.spark.csv", "year:int,make:string,model:string,comment:string,blank:string", "v1", "", "desc")
        case None => throw new IOException(s"Unknown dataset, datasetId={datasetId}")
      }
      case None => throw new IOException(s"Unknown repository, repoId={repoId}" )
    }
  }

  def get(repoId:String, datasetId: String): DataFrame = {
    val datasetMetadata = getMetadata(repoId, datasetId)
    log.info(s"DatasetMetadata:${datasetMetadata}")
    sqlContext.read.format("org.apache.spark.sql.dataset")
      .options(datasetMetadata.toOptions()).load()
  }
}

object DatasetLoader {
  def main(args:Array[String]): Unit = {
    println("hello world")
  }
}