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

package org.apache.carbondata.mv

import java.io.Closeable
import java.math.BigInteger

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.internal.{SessionState, SharedState}
import org.apache.carbondata.mv.rewrite.QueryRewrite

/**
 * The entry point for working with multi-query optimization in Sparky. Allow the 
 * creation of CSEs (covering subexpression) as well as query rewrite before
 * submitting to SparkSQL
 */
class MQOSession private(
    @transient val spark: SparkSession,
    @transient private val existingSharedState: Option[SharedState])
  extends Serializable with Closeable {  
  
  self =>
    
  def this(spark: SparkSession) = {
    this(spark, None)
  }
  
  //spark.sparkContext.assertNotStopped()
  
  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */
  
  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  private[mv] lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(spark.sparkContext))
  }
  
  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  private[mv] lazy val sessionState: SessionState = new SessionState(self)
//  private[sql] lazy val sessionState: SessionState = {
//    SparkSession.reflect[SessionState, SparkSession](
//      SparkSession.sessionStateClassName(sparkContext.conf),
//      self)
//  }
   
  @transient
  lazy val tableFrequencyMap = new mutable.HashMap[String,Int]
  
  @transient
  lazy val consumersMap = new mutable.HashMap[BigInteger, mutable.Set[LogicalPlan]] with mutable.MultiMap[BigInteger, LogicalPlan]
  
  def rewrite(sqlText: String): QueryRewrite = {
    val plan1 = spark.sql(sqlText).queryExecution.analyzed
    sessionState.rewritePlan(plan1)
  }
  
  override def close(): Unit = spark.close()
  
}

/**
 * This MQOSession object contains utility functions to create a singleton MQOSession instance,
 * or to get the last created MQOSession instance.
 */

object MQOSession {
//  
//  private val INSTANTIATION_LOCK = new Object()
//  
//  @transient private val lastInstantiatedContext = new AtomicReference[MQOContext]()
//  
//  def getOrCreate(sqlContext: SQLContext): MQOContext = {
//    INSTANTIATION_LOCK.synchronized {
//      if (lastInstantiatedContext.get() == null) {
//        new MQOContext(sqlContext)
//      }
//    }
//    lastInstantiatedContext.get()
//  }
//  
//  private[mqo] def clearLastInstantiatedContext(): Unit = {
//    INSTANTIATION_LOCK.synchronized {
//      lastInstantiatedContext.set(null)
//    }
//  }
//  
//  private[mqo] def setLastInstantiatedContext(mqoContext: MQOContext): Unit = {
//    INSTANTIATION_LOCK.synchronized {
//      lastInstantiatedContext.set(mqoContext)
//    }
//  }
}
