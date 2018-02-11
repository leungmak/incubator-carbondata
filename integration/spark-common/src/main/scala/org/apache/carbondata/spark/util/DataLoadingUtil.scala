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

package org.apache.carbondata.spark.util

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.{CarbonLoaderUtil, DeleteLoadFolders, TableOptionConstant}
import org.apache.carbondata.spark.load.ValidateUtil

/**
 * the util object of data loading
 */
object DataLoadingUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * check whether using default value or not
   */
  private def checkDefaultValue(value: String, default: String) = {
    if (StringUtils.isEmpty(value)) {
      default
    } else {
      value
    }
  }

  /**
   * build CarbonLoadModel for data loading
   * @param table CarbonTable object containing all metadata information for the table
   *              like table name, table path, schema, etc
   * @param options Load options from user input
   * @return a new CarbonLoadModel instance
   */
  def buildCarbonLoadModelJava(
      table: CarbonTable,
      options: java.util.Map[String, String]
  ): CarbonLoadModel = {
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance
    val optionsFinal = getDataLoadingOptions(carbonProperty, options.asScala.toMap)
    optionsFinal.put("sort_scope", "no_sort")
    if (!options.containsKey("fileheader")) {
      val csvHeader = table.getCreateOrderColumn(table.getTableName)
        .asScala.map(_.getColName).mkString(",")
      optionsFinal.put("fileheader", csvHeader)
    }
    val model = new CarbonLoadModel()
    buildCarbonLoadModel(
      table = table,
      carbonProperty = carbonProperty,
      options = options.asScala.toMap,
      optionsFinal = optionsFinal,
      carbonLoadModel = model,
      hadoopConf = null)  // we have provided 'fileheader', so it can be null

    // set default values
    model.setTimestampformat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    model.setDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    model.setUseOnePass(options.asScala.getOrElse("onepass", "false").toBoolean)
    model.setDictionaryServerHost(options.asScala.getOrElse("dicthost", null))
    model.setDictionaryServerPort(options.asScala.getOrElse("dictport", "-1").toInt)
    model
  }

  /**
   * build CarbonLoadModel for data loading
   * @param table CarbonTable object containing all metadata information for the table
   *              like table name, table path, schema, etc
   * @param carbonProperty Carbon property instance
   * @param options Load options from user input
   * @param optionsFinal Load options that populated with default values for optional options
   * @param carbonLoadModel The output load model
   * @param hadoopConf hadoopConf is needed to read CSV header if there 'fileheader' is not set in
   *                   user provided load options
   */
  def buildCarbonLoadModel(
      table: CarbonTable,
      carbonProperty: CarbonProperties,
      options: immutable.Map[String, String],
      optionsFinal: mutable.Map[String, String],
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration): Unit = {
    carbonLoadModel.setTableName(table.getTableName)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTablePath(table.getTablePath)
    carbonLoadModel.setTableName(table.getTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    val sort_scope = optionsFinal("sort_scope")
    val single_pass = optionsFinal("single_pass")
    val bad_records_logger_enable = optionsFinal("bad_records_logger_enable")
    val bad_records_action = optionsFinal("bad_records_action")
    var bad_record_path = optionsFinal("bad_record_path")
    val global_sort_partitions = optionsFinal("global_sort_partitions")
    val timestampformat = optionsFinal("timestampformat")
    val dateFormat = optionsFinal("dateformat")
    val delimeter = optionsFinal("delimiter")
    val complex_delimeter_level1 = optionsFinal("complex_delimiter_level_1")
    val complex_delimeter_level2 = optionsFinal("complex_delimiter_level_2")
    val all_dictionary_path = optionsFinal("all_dictionary_path")
    val column_dict = optionsFinal("columndict")
    ValidateUtil.validateDateTimeFormat(timestampformat, "TimestampFormat")
    ValidateUtil.validateDateTimeFormat(dateFormat, "DateFormat")
    ValidateUtil.validateSortScope(table, sort_scope)

    if (bad_records_logger_enable.toBoolean ||
        LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
      bad_record_path = CarbonUtil.checkAndAppendHDFSUrl(bad_record_path)
      if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
        CarbonException.analysisException("Invalid bad records location.")
      }
    }
    carbonLoadModel.setBadRecordsLocation(bad_record_path)

    ValidateUtil.validateGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal("escapechar"), "\\"))
    carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal("quotechar"), "\""))
    carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal("commentchar"), "#"))

    // if there isn't file header in csv file and load sql doesn't provide FILEHEADER option,
    // we should use table schema to generate file header.
    var fileHeader = optionsFinal("fileheader")
    val headerOption = options.get("header")
    if (headerOption.isDefined) {
      // whether the csv file has file header
      // the default value is true
      val header = try {
        headerOption.get.toBoolean
      } catch {
        case ex: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "'header' option should be either 'true' or 'false'. " + ex.getMessage)
      }
      if (header) {
        if (fileHeader.nonEmpty) {
          throw new MalformedCarbonCommandException(
            "When 'header' option is true, 'fileheader' option is not required.")
        }
      } else {
        if (fileHeader.isEmpty) {
          fileHeader = table.getCreateOrderColumn(table.getTableName)
            .asScala.map(_.getColName).mkString(",")
        }
      }
    }

    carbonLoadModel.setTimestampformat(timestampformat)
    carbonLoadModel.setDateFormat(dateFormat)
    carbonLoadModel.setDefaultTimestampFormat(carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))

    carbonLoadModel.setDefaultDateFormat(carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))

    carbonLoadModel.setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," +
        optionsFinal("serialization_null_format"))

    carbonLoadModel.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + bad_records_logger_enable)

    carbonLoadModel.setBadRecordsAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + bad_records_action.toUpperCase)

    carbonLoadModel.setIsEmptyDataBadRecord(
        DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," +
        optionsFinal("is_empty_data_bad_record"))

    carbonLoadModel.setSkipEmptyLine(optionsFinal("skip_empty_line"))

    carbonLoadModel.setSortScope(sort_scope)
    carbonLoadModel.setBatchSortSizeInMb(optionsFinal("batch_sort_size_inmb"))
    carbonLoadModel.setGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setUseOnePass(single_pass.toBoolean)

    if (delimeter.equalsIgnoreCase(complex_delimeter_level1) ||
        complex_delimeter_level1.equalsIgnoreCase(complex_delimeter_level2) ||
        delimeter.equalsIgnoreCase(complex_delimeter_level2)) {
      CarbonException.analysisException(s"Field Delimiter and Complex types delimiter are same")
    } else {
      carbonLoadModel.setComplexDelimiterLevel1(
        CarbonUtil.delimiterConverter(complex_delimeter_level1))
      carbonLoadModel.setComplexDelimiterLevel2(
        CarbonUtil.delimiterConverter(complex_delimeter_level2))
    }
    // set local dictionary path, and dictionary file extension
    carbonLoadModel.setAllDictPath(all_dictionary_path)
    carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimeter))
    carbonLoadModel.setCsvHeader(fileHeader)
    carbonLoadModel.setColDictFilePath(column_dict)
    carbonLoadModel.setCsvHeaderColumns(
      CommonUtil.getCsvHeaderColumns(carbonLoadModel, hadoopConf))

    val validatedMaxColumns = CommonUtil.validateMaxColumns(
      carbonLoadModel.getCsvHeaderColumns,
      optionsFinal("maxcolumns"))

    carbonLoadModel.setMaxColumns(validatedMaxColumns.toString)
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }
  }

  private def isLoadDeletionRequired(metaDataLocation: String): Boolean = {
    val details = SegmentStatusManager.readLoadMetadata(metaDataLocation)
    if (details != null && details.nonEmpty) for (oneRow <- details) {
      if ((SegmentStatus.MARKED_FOR_DELETE == oneRow.getSegmentStatus ||
           SegmentStatus.COMPACTED == oneRow.getSegmentStatus ||
           SegmentStatus.INSERT_IN_PROGRESS == oneRow.getSegmentStatus ||
           SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == oneRow.getSegmentStatus) &&
          oneRow.getVisibility.equalsIgnoreCase("true")) {
        return true
      }
    }
    false
  }

  def deleteLoadsAndUpdateMetadata(
      isForceDeletion: Boolean,
      carbonTable: CarbonTable): Unit = {
    if (isLoadDeletionRequired(carbonTable.getMetadataPath)) {
      val details = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(
          absoluteTableIdentifier,
          LockUsage.TABLE_STATUS_LOCK
        )

      // Delete marked loads
      val isUpdationRequired =
        DeleteLoadFolders.deleteLoadFoldersFromFileSystem(
          absoluteTableIdentifier,
          isForceDeletion,
          details,
          carbonTable.getMetadataPath
        )

      var updationCompletionStaus = false

      if (isUpdationRequired) {
        try {
          // Update load metadate file after cleaning deleted nodes
          if (carbonTableStatusLock.lockWithRetries()) {
            LOGGER.info("Table status lock has been successfully acquired.")

            // read latest table status again.
            val latestMetadata = SegmentStatusManager
              .readLoadMetadata(carbonTable.getMetadataPath)

            // update the metadata details from old to new status.
            val latestStatus = CarbonLoaderUtil
              .updateLoadMetadataFromOldToNew(details, latestMetadata)

            CarbonLoaderUtil.writeLoadMetadata(absoluteTableIdentifier, latestStatus)
          } else {
            val dbName = absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName
            val tableName = absoluteTableIdentifier.getCarbonTableIdentifier.getTableName
            val errorMsg = "Clean files request is failed for " +
                           s"$dbName.$tableName" +
                           ". Not able to acquire the table status lock due to other operation " +
                           "running in the background."
            LOGGER.audit(errorMsg)
            LOGGER.error(errorMsg)
            throw new Exception(errorMsg + " Please try after some time.")
          }
          updationCompletionStaus = true
        } finally {
          CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK)
        }
        if (updationCompletionStaus) {
          DeleteLoadFolders
            .physicalFactAndMeasureMetadataDeletion(absoluteTableIdentifier,
              carbonTable.getMetadataPath, isForceDeletion)
        }
      }
    }
  }

}
