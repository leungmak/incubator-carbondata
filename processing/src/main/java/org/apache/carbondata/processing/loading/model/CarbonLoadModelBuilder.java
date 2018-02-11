package org.apache.carbondata.processing.loading.model;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.Maps;
import org.apache.carbondata.common.Strings;
import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.sort.SortScopeOptions;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.processing.util.TableOptionConstant;
import org.apache.carbondata.spark.util.CommonUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.util.CarbonException;

public class CarbonLoadModelBuilder {

  private CarbonTable table;

  public CarbonLoadModelBuilder(CarbonTable table) {
    this.table = table;
  }

  /**
   * get data loading options and initialise default value
   */
  private Map<String, String> getDataLoadingOptions(
      CarbonProperties carbonProperty,
      Map<String, String> options) throws InvalidLoadOptionException {
    Map<String, String> optionsFinal = new HashMap<>();
    optionsFinal.put("delimiter", Maps.getOrDefault(options, "delimiter", ","));
    optionsFinal.put("quotechar", Maps.getOrDefault(options, "quotechar", "\""));
    optionsFinal.put("fileheader", Maps.getOrDefault(options, "fileheader", ""));
    optionsFinal.put("commentchar", Maps.getOrDefault(options, "commentchar", "#"));
    optionsFinal.put("columndict", Maps.getOrDefault(options, "columndict", null));

    optionsFinal.put(
        "escapechar",
        CarbonLoaderUtil.getEscapeChar(Maps.getOrDefault(options,"escapechar", "\\")));

    optionsFinal.put(
        "serialization_null_format",
        Maps.getOrDefault(options, "serialization_null_format", "\\N"));

    optionsFinal.put(
        "bad_records_logger_enable",
        Maps.getOrDefault(
            options,
            "bad_records_logger_enable",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT)));

    String badRecordActionValue = carbonProperty.getProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);

    optionsFinal.put(
        "bad_records_action",
        Maps.getOrDefault(
            options,
            "bad_records_action",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
                badRecordActionValue)));

    optionsFinal.put(
        "is_empty_data_bad_record",
        Maps.getOrDefault(
            options,
            "is_empty_data_bad_record",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT)));

    optionsFinal.put(
        "skip_empty_line",
        Maps.getOrDefault(
            options,
            "skip_empty_line",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_SKIP_EMPTY_LINE)));

    optionsFinal.put(
        "all_dictionary_path",
        Maps.getOrDefault(options, "all_dictionary_path", ""));

    optionsFinal.put(
        "complex_delimiter_level_1",
        Maps.getOrDefault(options,"complex_delimiter_level_1", "\\$"));

    optionsFinal.put(
        "complex_delimiter_level_2",
        Maps.getOrDefault(options, "complex_delimiter_level_2", "\\:"));

    optionsFinal.put(
        "dateformat",
        Maps.getOrDefault(
            options,
            "dateformat",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)));

    optionsFinal.put(
        "timestampformat",
        Maps.getOrDefault(
            options,
            "timestampformat",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT_DEFAULT)));

    optionsFinal.put(
        "global_sort_partitions",
        Maps.getOrDefault(
            options,
            "global_sort_partitions",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS,
                null)));

    optionsFinal.put("maxcolumns", Maps.getOrDefault(options, "maxcolumns", null));

    optionsFinal.put(
        "batch_sort_size_inmb",
        Maps.getOrDefault(
            options,
            "batch_sort_size_inmb",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
                carbonProperty.getProperty(
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))));

    optionsFinal.put(
        "bad_record_path",
        Maps.getOrDefault(
            options,
            "bad_record_path",
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
                carbonProperty.getProperty(
                    CarbonCommonConstants.CARBON_BADRECORDS_LOC,
                    CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))));

    String useOnePass = Maps.getOrDefault(
        options,
        "single_pass",
        carbonProperty.getProperty(
            CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
            CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT)).trim().toLowerCase();

    boolean singlePass;

    if (useOnePass.equalsIgnoreCase("true")) {
      singlePass = true;
    } else {
        // when single_pass = false  and if either alldictionarypath
        // or columnDict is configured the do not allow load
        if (StringUtils.isNotEmpty(optionsFinal.get("all_dictionary_path")) ||
            StringUtils.isNotEmpty(optionsFinal.get("columndict"))) {
          throw new InvalidLoadOptionException(
              "Can not use all_dictionary_path or columndict without single_pass.");
        } else {
          singlePass = false;
        }
    }

    optionsFinal.put("single_pass", String.valueOf(singlePass));
    return optionsFinal;
  }

  /**
   * build CarbonLoadModel for data loading
   * @param table CarbonTable object containing all metadata information for the table
   *              like table name, table path, schema, etc
   * @param options Load options from user input
   * @return a new CarbonLoadModel instance
   */
  public CarbonLoadModel build(
      Map<String, String> options) throws InvalidLoadOptionException {
    CarbonProperties carbonProperty = CarbonProperties.getInstance();
    Map<String, String> optionsFinal = getDataLoadingOptions(carbonProperty, options);
    optionsFinal.put("sort_scope", "no_sort");
    if (!options.containsKey("fileheader")) {
      List<CarbonColumn> csvHeader = table.getCreateOrderColumn(table.getTableName());
      String[] columns = new String[csvHeader.size()];
      for (int i = 0; i < columns.length; i++) {
        columns[i] = csvHeader.get(i).getColName();
      }
      optionsFinal.put("fileheader", Strings.mkString(columns, ","));
    }
    CarbonLoadModel model = new CarbonLoadModel();
    buildCarbonLoadModel(
        table,
        carbonProperty,
        options,
        optionsFinal,
        model,
        null);  // we have provided 'fileheader', so it can be null

    // set default values
    model.setTimestampformat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    model.setDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    model.setUseOnePass(Boolean.valueOf(Maps.getOrDefault(options, "onepass", "false")));
    model.setDictionaryServerHost(Maps.getOrDefault(options, "dicthost", null));
    model.setDictionaryServerPort(
        Integer.valueOf(Maps.getOrDefault(options, "dictport", "-1")));
    return model;
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
      CarbonProperties carbonProperty,
      Map<String, String> options,
      Map<String, String> optionsFinal,
      CarbonLoadModel carbonLoadModel,
      Configuration hadoopConf) throws MalformedCarbonCommandException {
    carbonLoadModel.setTableName(table.getTableName());
    carbonLoadModel.setDatabaseName(table.getDatabaseName());
    carbonLoadModel.setTablePath(table.getTablePath());
    carbonLoadModel.setTableName(table.getTableName());
    CarbonDataLoadSchema dataLoadSchema = new CarbonDataLoadSchema(table);
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema);
    String sort_scope = optionsFinal.get("sort_scope");
    String single_pass = optionsFinal.get("single_pass");
    String bad_records_logger_enable = optionsFinal.get("bad_records_logger_enable");
    String bad_records_action = optionsFinal.get("bad_records_action");
    String bad_record_path = optionsFinal.get("bad_record_path");
    String global_sort_partitions = optionsFinal.get("global_sort_partitions");
    String timestampformat = optionsFinal.get("timestampformat");
    String dateFormat = optionsFinal.get("dateformat");
    String delimeter = optionsFinal.get("delimiter");
    String complex_delimeter_level1 = optionsFinal.get("complex_delimiter_level_1");
    String complex_delimeter_level2 = optionsFinal.get("complex_delimiter_level_2");
    String all_dictionary_path = optionsFinal.get("all_dictionary_path");
    String column_dict = optionsFinal.get("columndict");
    validateDateTimeFormat(timestampformat);
    validateDateTimeFormat(dateFormat);
    validateSortScope(sort_scope);

    if (bad_records_logger_enable.toBoolean ||
        LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
      bad_record_path = CarbonUtil.checkAndAppendHDFSUrl(bad_record_path)
      if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
        CarbonException.analysisException("Invalid bad records location.")
      }
    }
    carbonLoadModel.setBadRecordsLocation(bad_record_path);

    validateGlobalSortPartitions(global_sort_partitions);
    carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal.get("escapechar"), "\\"))
    carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal.get("quotechar"), "\""))
    carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal.get("commentchar"), "#"))

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


  /**
   * validates both timestamp and date for illegal values
   */
  private void validateDateTimeFormat(String dateTimeLoadFormat)
      throws MalformedCarbonCommandException {
    // allowing empty value to be configured for dateformat option.
    if (dateTimeLoadFormat != null && !dateTimeLoadFormat.trim().equalsIgnoreCase("")) {
      try {
        new SimpleDateFormat(dateTimeLoadFormat);
      } catch (IllegalArgumentException e){
          throw new InvalidLoadOptionException(
              "Error: Wrong option: $dateTimeLoadFormat is" +
              " provided for option $dateTimeLoadOption");
      }
    }
  }

  private void validateSortScope(String sortScope) throws InvalidLoadOptionException {
    if (sortScope != null) {
      // Don't support use global sort on partitioned table.
      if (table.getPartitionInfo(table.getTableName()) != null &&
          sortScope.equalsIgnoreCase(SortScopeOptions.SortScope.GLOBAL_SORT.toString())) {
        throw new InvalidLoadOptionException("Don't support use global sort on partitioned table.");
      }
    }
  }

  private void validateGlobalSortPartitions(String globalSortPartitions)
      throws InvalidLoadOptionException {
    if (globalSortPartitions != null) {
      try {
        int num = Integer.valueOf(globalSortPartitions);
        if (num <= 0) {
          throw new InvalidLoadOptionException("'GLOBAL_SORT_PARTITIONS' should be greater than 0");
        }
      } catch (NumberFormatException e) {
        throw new InvalidLoadOptionException(e.getMessage());
      }
    }
  }

  /**
   * check whether using default value or not
   */
  private String checkDefaultValue(String value, String defaultValue) {
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    } else {
      return value;
    }
  }
}
