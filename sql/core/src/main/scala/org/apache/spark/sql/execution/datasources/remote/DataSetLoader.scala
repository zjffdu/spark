package org.apache.spark.sql.execution.datasources.remote

import java.io.IOException

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.collection.mutable
import scala.collection.immutable.Map
import org.apache.spark.{SparkContext, SparkConf, Logging}

case class RepositoryMetadata(id: String, url:String, desc: String, lastUpdated:String,
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

  private val repositories: scala.collection.mutable.Map[String, RepositoryMetadata] =
    new mutable.HashMap[String, RepositoryMetadata]()


  def register(repoId: String, url: String): Unit = {
    // should use the url to fetch metadata or this repository
    // hard code here for poc
    val metadata = RepositoryMetadata(repoId, url, "desc", "2010-01-01",
      (1 to 10).map(e=>("a" + e +"a" -> "http://dummy-url-for-dataset")).toMap)
    this.repositories += repoId -> metadata
  }

  // register the built-in repositories
  register("libsvm", "http://libsvm")
  register("uci", "http://uci")

  // for poc
  private val a1a = DatasetMetadata("a1a", Seq("https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a1a"),
    "libsvm", "", "1.0", "2010-01-01", "desc")
  private val a2a = DatasetMetadata("a1a", Seq("https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a2a"),
    "libsvm", "", "1.0", "2010-01-01", "desc")
  private val a3a = DatasetMetadata("a1a", Seq("https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a3a"),
    "libsvm", "", "1.0", "2010-01-01", "desc")
  private val a4a = DatasetMetadata("a1a", Seq("https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a4a"),
    "libsvm", "", "1.0", "2010-01-01", "desc")

  private val datasetForPOC = Map("a1a" -> a1a, "a2a" -> a2a, "a3a" -> a3a, "a4a" -> a4a)

  def list(repoId: String, extended: Boolean = false): Seq[DatasetMetadata] = {
    repositories.get(repoId) match {
      case Some(url) =>
        Seq(a1a, a2a, a3a, a4a)
      case None => throw new IOException(s"No such repository, repoId={repoId}")
    }
  }

  def desc(repoId:String, datasetId: String): DatasetMetadata = {
    repositories.get(repoId) match {
      case Some(repo) => repo.datasets.get(datasetId) match {
        case Some(datasetURL) =>
          // fetch dataset metadata through the datasetURL
          // hard code here for poc
          a1a
        case None => throw new IOException(s"Unknown dataset, datasetId=${datasetId}")
      }
      case None => throw new IOException(s"Unknown repository, repoId=${repoId}")
    }
  }

  private def getMetadata(repoId: String, datasetId: String) : DatasetMetadata = {
    repositories.get(repoId) match {
      case Some(repoMetadata) => repoMetadata.datasets.get(datasetId) match {
        case Some(datasetURL) =>
          // try this url to fetch dataset metadata
          // hard code here for poc
          datasetForPOC(datasetId)
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



  }
}