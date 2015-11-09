package org.apache.spark.sql.execution.datasources.remote

import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, DataType, StructType}
import org.apache.spark.util.SerializableConfiguration



class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "dataset"

  /**
   * Returns a new base relation with the given parameters and user defined schema.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val metadata = DatasetMetadata(
      parameters("remote.id"),
      parameters("remote.urls").split(",").toSeq,
      parameters("remote.format"),
      parameters("remote.schema"),
      parameters("remote.version"),
      parameters("remote.lastUpdated"),
      parameters("remote.desc")
    )
    val remotePath = sqlContext.conf.getConfString("remote.root.dir", "/tmp/remote/") + parameters("remote.id")
    new RemoteRelation(sqlContext, metadata, parameters.filter(!_._1.startsWith("remote."))
      + ("path"-> remotePath),
      remotePath)
  }
}

@Experimental
class RemoteRelation(val sqlContext: SQLContext, metadata: DatasetMetadata,
                     options:Map[String, String], remotePath: String)
  extends HadoopFsRelation with Logging {

  private val delegatedRelation: HadoopFsRelation = ResolvedDataSource(
      sqlContext,
      userSpecifiedSchema = metadata.schema,
      partitionColumns = Array.empty[String],
      provider = metadata.format,
      options).relation.asInstanceOf[HadoopFsRelation]

  private val _dataSchema = metadata.schema

  override def prepareJobForWrite(job: _root_.org.apache.hadoop.mapreduce.Job): _root_.org.apache.spark.sql.sources.OutputWriterFactory = ???

  override def paths: Array[String] = metadata.urls.toArray

  override lazy val schema = delegatedRelation match {
    case a : HadoopFsRelation => delegatedRelation.asInstanceOf[HadoopFsRelation].schema
    case _ => delegatedRelation.schema
  }

  override def dataSchema: StructType = delegatedRelation match {
    case a : HadoopFsRelation => delegatedRelation.asInstanceOf[HadoopFsRelation].dataSchema
    case _ => delegatedRelation.schema
  }

  private val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)

  def checkCache() = {

    // always delete it now, just for testing
    fs.delete(new Path(remotePath), true)
    if (!fs.exists(new Path(remotePath))) {
      fs.mkdirs(new Path(remotePath))
    }
    if (fs.listStatus(new Path(remotePath)).isEmpty) {
      logInfo("Downloading data from urls:" + metadata.urls)
      download(metadata.urls, remotePath)
      logInfo("Downloading done")
    }
  }

  def download(urls:Seq[String], destFolder:String): Unit = {
    RestUtils.download(urls, destFolder, fs)
  }

  override private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {

    checkCache()
    delegatedRelation.asInstanceOf[HadoopFsRelation].buildScan(requiredColumns, filters, inputFiles)
  }

  override lazy val fileStatusCache = {
    val cache = new FileStatusCache
    cache
  }

//  override def buildScan(): RDD[Row] = {
//    checkCache()
//    delegatedRelation.asInstanceOf[TableScan].buildScan()
//  }
}
