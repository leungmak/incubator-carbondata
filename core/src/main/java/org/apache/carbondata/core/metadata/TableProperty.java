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
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.util.CarbonUtil;

public class TableProperty {

  public static List<String> getSortColumns(
      StructType schema,
      Map<String, String> tableProperties) {
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
        if (field.isDimension(tableProperties) && !field.getDataType().isComplexType()) {
          sortColumns.add(field.getFieldName());
        }
      }
    }
    return sortColumns;
  }

  public static List<String> getNoInvertedIndexColumns(
      Map<String, String> tableProperties) {
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

  public static List<String> getDictionaryColumns(
      Map<String, String> tableProperties) {
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
