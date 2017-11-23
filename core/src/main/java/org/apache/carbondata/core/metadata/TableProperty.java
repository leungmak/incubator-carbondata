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

package org.apache.carbondata.core.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapField;
import org.apache.carbondata.core.util.CarbonUtil;

public class TableProperty {

  private StructType schema;
  private Map<String, String> tableProperties;
  private List<String> sortColumns;
  private List<String> noInvertedIndexColumns;
  private List<String> dictionaryColumns;

  public TableProperty(StructType schema, Map<String, String> tableProperties) {
    this.schema = schema;
    this.tableProperties = tableProperties;
    this.noInvertedIndexColumns = extractNoInvertedIndexColumns();
    this.dictionaryColumns = extractDictionaryColumns();
    this.sortColumns = extractSortColumns();
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public List<String> getNoInvertedIndexColumns() {
    return noInvertedIndexColumns;
  }

  public List<String> getDictionaryColumns() {
    return dictionaryColumns;
  }

  private List<String> extractSortColumns() {
    String sortColumnsString = tableProperties.get(CarbonCommonConstants.SORT_COLUMNS);
    List<String> sortColumns = new ArrayList<>();
    if (sortColumnsString != null) {
      String[] columns =
          CarbonUtil.unquoteChar(sortColumnsString.toLowerCase()).trim().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      sortColumns = Arrays.asList(columns);
    }
    if (sortColumns.isEmpty()) {
      // default sort columns is all dimension except complex type
      List<StructField> fields = schema.getFields();
      for (StructField field : fields) {
        DataType dataType = field.getDataType();
        if (dictionaryColumns.contains(field.getFieldName().toLowerCase()) &&
            !field.getDataType().isComplexType()) {
          sortColumns.add(field.getFieldName());
        }
        if (dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE) {
          sortColumns.add(field.getFieldName());
        }
      }
    }
    return sortColumns;
  }

  private List<String> extractNoInvertedIndexColumns() {
    // Column names that does not do inverted index.
    // Note that inverted index is allowed only in sort columns
    String noInvertedIndexString = tableProperties.get(CarbonCommonConstants.NO_INVERTED_INDEX);
    if (noInvertedIndexString == null) {
      return new ArrayList<>(0);
    } else {
      String[] columns = noInvertedIndexString.toLowerCase().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      return Arrays.asList(columns);
    }
  }

  private List<String> extractDictionaryColumns() {
    String dictionaryColumns = tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE);
    if (dictionaryColumns == null) {
      return new ArrayList<>(0);
    } else {
      String[] columns = dictionaryColumns.toLowerCase().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      return Arrays.asList(columns);
    }
  }

}
