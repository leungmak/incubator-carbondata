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

package org.apache.spark.sql.execution.command

import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, TableProperty}
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes, DecimalType, StructField}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema._
import org.apache.carbondata.core.metadata.schema.table.{BucketFields, CarbonTable, RelationIdentifier, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.{ColumnSchema, ParentColumnTableRelation}
import org.apache.carbondata.core.service.CarbonCommonFactory
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CompactionType
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.util.DataTypeConverterUtil

case class ColumnProperty(key: String, value: String)

case class ComplexField(complexType: String, primitiveField: Option[StructField],
    complexField: Option[ComplexField])

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class ExecutionErrors(var failureCauses: FailureCauses, var errorMsg: String )

case class CarbonMergerMapping(
    hdfsStoreLocation: String,
    metadataFilePath: String,
    var mergedLoadName: String,
    databaseName: String,
    factTableName: String,
    validSegments: Array[String],
    tableId: String,
    campactionType: CompactionType,
    // maxSegmentColCardinality is Cardinality of last segment of compaction
    var maxSegmentColCardinality: Array[Int],
    // maxSegmentColumnSchemaList is list of column schema of last segment of compaction
    var maxSegmentColumnSchemaList: List[ColumnSchema])

case class NodeInfo(TaskId: String, noOfBlocks: Int)

case class AlterTableModel(
    dbName: Option[String],
    tableName: String,
    segmentUpdateStatusManager: Option[SegmentUpdateStatusManager],
    compactionType: String,
    factTimeStamp: Option[Long],
    var alterSql: String)

case class UpdateTableModel(
    isUpdate: Boolean,
    updatedTimeStamp: Long,
    var executorErrors: ExecutionErrors)

case class CompactionModel(compactionSize: Long,
    compactionType: CompactionType,
    carbonTable: CarbonTable,
    isDDLTrigger: Boolean)

case class CompactionCallableModel(carbonLoadModel: CarbonLoadModel,
    carbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails],
    sqlContext: SQLContext,
    compactionType: CompactionType)

case class AlterPartitionModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext
)

case class SplitPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    partitionId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext)

case class DropPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    partitionId: String,
    oldPartitionIds: List[Int],
    dropWithData: Boolean,
    carbonTable: CarbonTable,
    sqlContext: SQLContext)

case class DataTypeInfo(dataType: String, precision: Int = 0, scale: Int = 0)

case class AlterTableDataTypeChangeModel(dataTypeInfo: DataTypeInfo,
    databaseName: Option[String],
    tableName: String,
    columnName: String,
    newColumnName: String)

case class AlterTableRenameModel(
    oldTableIdentifier: TableIdentifier,
    newTableIdentifier: TableIdentifier
)

case class AlterTableAddColumnsModel(
    databaseName: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[StructField],
    msrCols: Seq[StructField],
    highCardinalityDims: Seq[String])

case class AlterTableDropColumnModel(databaseName: Option[String],
    tableName: String,
    columns: List[String])

case class AlterTableDropPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    dropWithData: Boolean)

case class AlterTableSplitPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    splitInfo: List[String])

class AlterTableColumnSchemaGenerator(
    dbName: String,
    tableName: String,
    newFields: Seq[StructField],
    tableProperties: Map[String, String],
    carbonTable: CarbonTable,
    carbonTablePath: CarbonTablePath,
    tableIdentifier: CarbonTableIdentifier,
    sc: SparkContext) {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def process: Seq[ColumnSchema] = {
    val allColumns = carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
    var newCols = Seq[ColumnSchema]()

    val sortColumns = carbonTable.getSortColumns(tableName)

    newFields.foreach { field =>
      // datamap does not support alter table, so parent table must be null
      newCols +:= field.createColumnSchema(sortColumns, tableProperties.asJava, null, null)
    }

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.filter(x => !x.isInvisible).groupBy(_.getColumnName)
      .foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate column found with name: $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for $dbName.$tableName. " +
        s"Duplicate column found with name: $name")
      sys.error(s"Duplicate column found with name: $name")
    })

    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator
    columnValidator.validateColumns(allColumns)

    // populate table properties map
    val oldProperties = carbonTable.getTableInfo.getFactTable.getTableProperties
    tableProperties.foreach {
      x => val value = oldProperties.get(x._1)
        if (null != value) {
          oldProperties.put(x._1, value + "," + x._2)
        } else {
          oldProperties.put(x._1, x._2)
        }
    }

    // This part will create dictionary file for all newly added dictionary columns
    // if valid default value is provided,
    // then that value will be included while creating dictionary file
    val defaultValueString = "default.value."
    newCols.foreach { col =>
      var rawData: String = null
      for (elem <- tableProperties) {
        if (elem._1.toLowerCase.startsWith(defaultValueString)) {
          if (col.getColumnName.equalsIgnoreCase(elem._1.substring(defaultValueString.length))) {
            rawData = elem._2
            val data = DataTypeUtil.convertDataToBytesBasedOnDataType(elem._2, col)
            if (null != data) {
              col.setDefaultValue(data)
            } else {
              LOGGER.error(
                s"Invalid default value for new column $dbName.$tableName.${ col.getColumnName }" +
                s" : ${ elem._2 }")
            }
          }
        }
        else if (elem._1.equalsIgnoreCase("no_inverted_index") &&
                 elem._2.split(",").contains(col.getColumnName)) {
          col.getEncodingList.remove(Encoding.INVERTED_INDEX)
        }
      }
    }
    carbonTable.getTableInfo.getFactTable.setListOfColumns(allColumns.asJava)
    carbonTable.getTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    newCols
  }

}

