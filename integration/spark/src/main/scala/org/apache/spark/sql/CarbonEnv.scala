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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonMetaData, CarbonMetastoreCatalog, CarbonSessionCatalog, CarbonSessionState, TableMeta}

/**
  * Carbon Environment for unified context
  */
case class CarbonEnv(sqlContext: SparkSession, var carbonCatalog: CarbonMetastoreCatalog)

object CarbonEnv {
  val className = classOf[CarbonEnv].getCanonicalName
  var carbonEnv: CarbonEnv = _

  def getInstance(sqlContext: SparkSession): CarbonEnv = {
    if (carbonEnv == null) {
      carbonEnv = CarbonEnv(sqlContext, sqlContext.sessionState.catalog.asInstanceOf[CarbonSessionCatalog].metastoreCatalog)
    }
    carbonEnv
  }

  def isExist(sqlContext: SparkSession, identifier: TableIdentifier): Boolean = {
    getInstance(sqlContext).carbonCatalog.getTableMeta(identifier) != null
  }

  def isExist(sessionState: CarbonSessionState, identifier: TableIdentifier): Boolean = {
    sessionState.catalog.metastoreCatalog.getTableMeta(identifier) != null
  }

  def getTableMeta(sqlContext: SparkSession, identifier: TableIdentifier): TableMeta = {
    getInstance(sqlContext).carbonCatalog.getTableMeta(identifier)
  }

  def getMetaData(sqlContext: SparkSession, identifier: TableIdentifier): CarbonMetaData = {
    getInstance(sqlContext).carbonCatalog.getMetaData(identifier)
  }
  def getMetaData(sessionState: CarbonSessionState, identifier: TableIdentifier): CarbonMetaData = {
    sessionState.catalog.metastoreCatalog.getMetaData(identifier)
  }
}