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
    val remotePath = sqlContext.conf.getConfString("remote.root.dir", "/user/jzhang/remote/") + parameters("remote.id")
    new RemoteRelation(sqlContext, metadata, parameters.filter(!_._1.startsWith("remote."))
      + ("path"-> remotePath), remotePath)
  }
}

@Experimental
class RemoteRelation(val sqlContext: SQLContext, metadata: DatasetMetadata,
                     options:Map[String, String], remotePath: String)
  extends HadoopFsRelation with TableScan with PrunedScan with Logging {

  private val delegatedRelation: BaseRelation = ResolvedDataSource(
      sqlContext,
      userSpecifiedSchema = metadata.schema,
      partitionColumns = Array.empty[String],
      provider = metadata.format,
      options).relation

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

  val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)

  def checkCache() = {

    // delete it for testing
    fs.delete(new Path(remotePath), true)
    if (!fs.exists(new Path(remotePath))) {
      fs.mkdirs(new Path(remotePath))
    }
    if (fs.listStatus(new Path(remotePath)).isEmpty) {
      logInfo("Downloading data from urls:" + metadata.urls)
      download(metadata.urls, remotePath)
    }
  }

  def download(urls:Seq[String], destFolder:String): Unit = {
    val output = fs.create(new Path(destFolder + "/a.txt"))
    import scala.io.Source
    urls.foreach(url=>{
      IOUtils.copyBytes(new ReaderInputStream(Source.fromURL(url).reader()), output, fs.getConf)
      output.close()
    })
  }

  override private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {

    checkCache()
    delegatedRelation.asInstanceOf[HadoopFsRelation].buildScan(requiredColumns, filters, inputFiles)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    checkCache()
    delegatedRelation.asInstanceOf[PrunedScan].buildScan(requiredColumns)
  }

  override def buildScan(): RDD[Row] = {
    checkCache()
    delegatedRelation.asInstanceOf[TableScan].buildScan()
  }
}
