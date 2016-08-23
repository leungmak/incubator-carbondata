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

package org.apache.spark.sql

import java.io.File

import scala.language.implicitConversions

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.execution.command.PartitionData
import org.apache.spark.sql.hive.{HiveSharedState, _}
import org.apache.spark.sql.internal.{SQLConf, SessionState, SharedState}
import org.apache.spark.sql.optimizer.LazyProjection

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties


class CarbonContext(
    val sc: SparkContext,
    val storePath: String,
    metaStorePath: String)
    extends SparkSession(sc) {
  self =>

  def this(sc: SparkContext) = {
    this (sc,
      new File(CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL).getCanonicalPath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  def this(sc: SparkContext, storePath: String) = {
    this(
      sc,
      storePath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  CarbonContext.addInstance(sc, this)
  CarbonEnv.getInstance(this).carbonCatalog =
      sessionState.asInstanceOf[CarbonSessionState].catalog.metastoreCatalog

  var lastSchemaUpdatedTime = System.currentTimeMillis()

  override private[sql] lazy val sessionState: SessionState = {
    new CarbonSessionState(this, storePath)
  }

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SharedState = {
    new CarbonSharedState(sc)
  }


  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass.getName)

  override def sql(sqlText: String): DataFrame = {
    // queryId will be unique for each query, creting query detail holder
    val queryId: String = System.nanoTime() + ""
    sc.conf.set("queryId", queryId)

    CarbonContext.updateCarbonPorpertiesPath(sc)
    val sqlString = sqlText.toUpperCase()
    LOGGER.info(s"Query [$sqlString]")
    super.sql(sqlString)
  }

}

object CarbonContext {

  val datasourceName: String = "org.apache.carbondata.format"

  val datasourceShortName: String = "carbondata"

  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass.getName)
  /**
   * @param databaseName - Database Name
   * @param tableName   - Table Name
   * @param factPath   - Raw CSV data path
   * @param targetPath - Target path where the file will be split as per partition
   * @param delimiter  - default file delimiter is comma(,)
   * @param quoteChar  - default quote character used in Raw CSV file, Default quote
   *                   character is double quote(")
   * @param fileHeader - Header should be passed if not available in Raw CSV File, else pass null,
   *                   Header will be read from CSV
   * @param escapeChar - This parameter by default will be null, there wont be any validation if
   *                   default escape character(\) is found on the RawCSV file
   * @param multiLine  - This parameter will be check for end of quote character if escape character
   *                   & quote character is set.
   *                   if set as false, it will check for end of quote character within the line
   *                   and skips only 1 line if end of quote not found
   *                   if set as true, By default it will check for 10000 characters in multiple
   *                   lines for end of quote & skip all lines if end of quote not found.
   */
  final def partitionData(
      databaseName: String = null,
      tableName: String,
      factPath: String,
      targetPath: String,
      delimiter: String = ",",
      quoteChar: String = "\"",
      fileHeader: String = null,
      escapeChar: String = null,
      multiLine: Boolean = false)(sparkSession: SparkSession): String = {
    updateCarbonPorpertiesPath(sparkSession.sparkContext)
    var databaseNameLocal = databaseName
    if (databaseNameLocal == null) {
      databaseNameLocal = "default"
    }
    val partitionDataClass = PartitionData(databaseName, tableName, factPath, targetPath, delimiter,
      quoteChar, fileHeader, escapeChar, multiLine)
    partitionDataClass.run(sparkSession)
    partitionDataClass.partitionStatus
  }

  final def updateCarbonPorpertiesPath(sc: SparkContext) {
    val carbonPropertiesFilePath = sc.conf.get("carbon.properties.filepath", null)
    val systemcarbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null != carbonPropertiesFilePath && null == systemcarbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        carbonPropertiesFilePath + "/" + "carbon.properties")
    }
    // configuring the zookeeper URl .
    val zooKeeperUrl = sc.conf.get("spark.deploy.zookeeper.url", "127.0.0.1:2181")

    CarbonProperties.getInstance().addProperty("spark.deploy.zookeeper.url", zooKeeperUrl)

  }

  // this cache is used to avoid creating multiple CarbonContext from same SparkContext,
  // to avoid the derby problem for metastore
  private val cache = collection.mutable.Map[SparkContext, CarbonContext]()

  def getInstance(sc: SparkContext): CarbonContext = {
    cache(sc)
  }

  def addInstance(sc: SparkContext, cc: CarbonContext): Unit = {
    if (cache.contains(sc)) {
      sys.error("creating multiple instances of CarbonContext is not " +
                "allowed using the same SparkContext instance")
    }
    cache(sc) = cc
  }

  /**
   *
   * Requesting the extra executors other than the existing ones.
    *
    * @param sc
   * @param numExecutors
   * @return
   */
  final def ensureExecutors(sc: SparkContext, numExecutors: Int): Boolean = {
    sc.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        val requiredExecutors = numExecutors // -  b.numExistingExecutors
//        LOGGER.info("number of executors is =" + numExecutors + " existing executors are =" + b
//            .numExistingExecutors
//          )
        if(requiredExecutors > 0) {
          b.requestExecutors(requiredExecutors)
        }
        true
      case _ =>
        false
    }

  }
}
