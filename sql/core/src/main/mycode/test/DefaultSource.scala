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

package org.apache.spark

import _root_.org.apache.hadoop.mapreduce.{TaskAttemptContext, Job}
import _root_.org.apache.spark.Logging
import _root_.org.apache.spark.mapred.SparkHadoopMapRedUtil
import _root_.org.apache.spark.sql.SQLContext
import _root_.org.apache.spark.sql.sources.{OutputWriter, OutputWriterFactory, HadoopFsRelation}
import _root_.org.apache.spark.sql.types.StructType

/**
 * Created by jzhang on 11/11/15.
 */
class DefaultSource {

}


class MyRelation extends HadoopFsRelation {
  /**
   * Base paths of this relation.  For partitioned relations, it should be either root directories
   * of all partition directories.
   *
   * @since 1.4.0
   */
  override def paths: Array[String] = ???

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   *
   * @since 1.4.0
   */
  override def dataSchema: StructType = ???

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
  override def prepareJobForWrite(job: Job): OutputWriterFactory = ???

  override def sqlContext: SQLContext = ???
}

import  org.apache.spark.sql.sources._

//private[json] class JsonOutputWriter(
//    path: String,
//    dataSchema: StructType,
//    context: TaskAttemptContext)
//  extends OutputWriter with SparkHadoopMapRedUtil with Logging {
//  /**
//   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
//   * the task output is committed.
//   *
//   * @since 1.4.0
//   */
//  override def close(): Unit = ???
//}