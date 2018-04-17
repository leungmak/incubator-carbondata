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

package org.apache.carbondata.store

import java.io.IOException
import java.net.InetAddress

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.store.master.Master
import org.apache.carbondata.store.worker.Worker

/**
 * A CarbonStore implementation that uses Spark as underlying compute engine
 * with CarbonData query optimization capability
 */
@InterfaceAudience.Internal
class SparkCarbonStore extends MetaCachedCarbonStore {
  private var session: SparkSession = _
  private var master: Master = _

  /**
   * Initialize SparkCarbonStore
   * @param storeName store name
   * @param storeLocation location to store data
   */
  def this(storeName: String, storeLocation: String) = {
    this()
    val sparkConf = new SparkConf(loadDefaults = true)
    session = SparkSession.builder
      .config(sparkConf)
      .appName("SparkCarbonStore-" + storeName)
      .config("spark.sql.warehouse.dir", storeLocation)
      .getOrCreateCarbonSession()
  }

  def this(sparkSession: SparkSession) = {
    this()
    session = sparkSession
  }

  @throws[IOException]
  override def scan(
      path: String,
      projectColumns: Array[String]): java.util.Iterator[CarbonRow] = {
    require(path != null)
    require(projectColumns != null)
    scan(path, projectColumns, null)
  }

  @throws[IOException]
  override def scan(
      path: String,
      projectColumns: Array[String],
      filter: Expression): java.util.Iterator[CarbonRow] = {
    require(path != null)
    require(projectColumns != null)
    if (master == null) {
      startSearchMode()
    }
    master.search(getTable(path), projectColumns, filter)
      .iterator
      .asJava
  }

  @throws[IOException]
  override def sql(sqlString: String): java.util.Iterator[CarbonRow] = {
    val df = session.sql(sqlString)
    df.rdd
      .map(row => new CarbonRow(row.toSeq.toArray.asInstanceOf[Array[Object]]))
      .collect()
      .iterator
      .asJava
  }

  def startSearchMode(): Unit = {
    master = new Master()
    master.startService()
    startAllWorkers()
  }

  def stopSearchMode(): Unit = {
    master.stopAllWorkers()
    master.stopService()
    master = null
  }

  private def startAllWorkers(): Array[Int] = {
    // TODO: how to ensure task is sent to every executor?
    val numExecutors = session.sparkContext.getExecutorMemoryStatus.keySet.size
    val masterHostname = InetAddress.getLocalHost.getHostName
    session.sparkContext.parallelize(1 to numExecutors * 10, numExecutors).mapPartitions { f =>
      // start worker
      val worker: Worker = Worker.getInstance()
      worker.init(masterHostname, Master.DEFAULT_PORT)
      new Iterator[Int] {
        override def hasNext: Boolean = false
        override def next(): Int = 1
      }
    }.collect()
  }

}
