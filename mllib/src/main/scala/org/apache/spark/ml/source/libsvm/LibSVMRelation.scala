/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.source.libsvm

import com.google.common.base.Objects
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.Logging
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * LibSVMRelation provides the DataFrame constructed from LibSVM format data.
 * @param path File path of LibSVM format
 * @param numFeatures The number of features
 * @param vectorType The type of vector. It can be 'sparse' or 'dense'
 * @param sqlContext The Spark SQLContext
 */
private[libsvm] class LibSVMRelation(val path: String, val numFeatures: Int, val vectorType: String)
    (@transient val sqlContext: SQLContext)
  extends HadoopFsRelation with Logging with Serializable {

//  override def schema: StructType = StructType(
//    StructField("label", DoubleType, nullable = false) ::
//      StructField("features", new VectorUDT(), nullable = false) :: Nil
//  )

//  override def buildScan(): RDD[Row] = {
//    val sc = sqlContext.sparkContext
//    val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)
//    val sparse = vectorType == "sparse"
//    baseRdd.map { pt =>
//      val features = if (sparse) pt.features.toSparse else pt.features.toDense
//      Row(pt.label, features)
//    }
//  }

   override def buildScan(requiredColumns: Array[String], inputFiles: Array[FileStatus]): RDD[Row] = {
     val sc = sqlContext.sparkContext
     val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)
     val sparse = vectorType == "sparse"
     baseRdd.map { pt =>
       val features = if (sparse) pt.features.toSparse else pt.features.toDense
       Row(pt.label, features)
     }
   }

  override def hashCode(): Int = {
    Objects.hashCode(path, Double.box(numFeatures), vectorType)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LibSVMRelation =>
      path == that.path &&
        numFeatures == that.numFeatures &&
        vectorType == that.vectorType
    case _ =>
      false
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   *
   * Note that the only side effect expected here is mutating `job` via its setters.  Especially,
   * Spark SQL caches [[BaseRelation]] instances for performance, mutating relation internal states
   * may cause unexpected behaviors.
   *
   * @since 1.4.0
   */
  override def prepareJobForWrite(job: _root_.org.apache.hadoop.mapreduce.Job): _root_.org.apache.spark.sql.sources.OutputWriterFactory = ???

  /**
   * Base paths of this relation.  For partitioned relations, it should be either root directories
   * of all partition directories.
   *
   * @since 1.4.0
   */
  override def paths: Array[String] = Array(path)

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   *
   * @since 1.4.0
   */
  override def dataSchema: StructType = StructType(
    StructField("label", DoubleType, nullable = false) ::
      StructField("features", new VectorUDT(), nullable = false) :: Nil)
}

/**
 * `libsvm` package implements Spark SQL data source API for loading LIBSVM data as [[DataFrame]].
 * The loaded [[DataFrame]] has two columns: `label` containing labels stored as doubles and
 * `features` containing feature vectors stored as [[Vector]]s.
 *
 * To use LIBSVM data source, you need to set "libsvm" as the format in [[DataFrameReader]] and
 * optionally specify options, for example:
 * {{{
 *   // Scala
 *   val df = sqlContext.read.format("libsvm")
 *     .option("numFeatures", "780")
 *     .load("data/mllib/sample_libsvm_data.txt")
 *
 *   // Java
 *   DataFrame df = sqlContext.read.format("libsvm")
 *     .option("numFeatures, "780")
 *     .load("data/mllib/sample_libsvm_data.txt");
 * }}}
 *
 * LIBSVM data source supports the following options:
 *  - "numFeatures": number of features.
 *    If unspecified or nonpositive, the number of features will be determined automatically at the
 *    cost of one additional pass.
 *    This is also useful when the dataset is already split into multiple files and you want to load
 *    them separately, because some features may not present in certain files, which leads to
 *    inconsistent feature dimensions.
 *  - "vectorType": feature vector type, "sparse" (default) or "dense".
 *
 *  @see [[https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/ LIBSVM datasets]]
 */
@Since("1.6.0")
class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  @Since("1.6.0")
  override def shortName(): String = "libsvm"

//  @Since("1.6.0")
//  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
//    : BaseRelation = {
//    val path = parameters.getOrElse("path",
//      throw new IllegalArgumentException("'path' must be specified"))
//    val numFeatures = parameters.getOrElse("numFeatures", "-1").toInt
//    val vectorType = parameters.getOrElse("vectorType", "sparse")
//    new LibSVMRelation(path, numFeatures, vectorType)(sqlContext)
//  }
  /**
   * Returns a new base relation with the given parameters, a user defined schema, and a list of
   * partition columns. Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   *
   * @param dataSchema Schema of data columns (i.e., columns that are not partition columns).
   */
  override def createRelation(sqlContext: SQLContext,
                              paths: Array[String],
                              dataSchema: Option[StructType],
                              partitionColumns: Option[StructType],
                              parameters: Map[String, String]): HadoopFsRelation = {
    val numFeatures = parameters.getOrElse("numFeatures", "-1").toInt
    val vectorType = parameters.getOrElse("vectorType", "sparse")
    new LibSVMRelation(paths(0), numFeatures, vectorType)(sqlContext)
  }
}
