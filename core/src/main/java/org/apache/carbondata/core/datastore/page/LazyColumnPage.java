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

package org.apache.carbondata.core.datastore.page;

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * This is a decorator of column page, it performs transformation lazily (when caller calls getXXX
 * method to get the value from the page)
 */
public class LazyColumnPage extends ColumnPage {

  // decorated column page
  private ColumnPage columnPage;

  // transform that will apply to page data in getXXX
  private ColumnPageTransform transform;

  private ColumnPageStatsVO stats;
  private BigDecimal maxDecimal;

  private LazyColumnPage(ColumnPage columnPage, ColumnPageTransform transform,
      ColumnPageStatsVO stats) {
    super(columnPage.getDataType(), columnPage.getPageSize());
    this.columnPage = columnPage;
    this.transform = transform;
    this.stats = stats;
    if (stats.getDataType() == DataType.DOUBLE) {
      this.maxDecimal = BigDecimal.valueOf((double) stats.getMax());
    }
  }

  public static ColumnPage newPage(ColumnPage columnPage, ColumnPageTransform transform,
      ColumnPageStatsVO stats) {
    return new LazyColumnPage(columnPage, transform, stats);
  }

  @Override
  public String toString() {
    return String.format("[transform: %s, data type: %s, stats: %s", transform,
        columnPage.getDataType().getName(), stats);
  }

  @Override
  public long getLong(int rowId) {
    switch (transform) {
      case NO_OP:
        switch (columnPage.getDataType()) {
          case BYTE:
            return columnPage.getByte(rowId);
          case SHORT:
            return columnPage.getShort(rowId);
          case INT:
            return columnPage.getInt(rowId);
          case LONG:
            return columnPage.getLong(rowId);
        }
        break;
      case MAX_DELTA:
        switch (columnPage.getDataType()) {
          case BYTE:
            if (stats.getDataType() == DataType.DOUBLE) {
              return (long) ((double) stats.getMax() - columnPage.getByte(rowId));
            } else {
              return (long) stats.getMax() - columnPage.getByte(rowId);
            }
          case SHORT:
            if (stats.getDataType() == DataType.DOUBLE) {
              return (long) ((double) stats.getMax() - columnPage.getShort(rowId));
            } else {
              return (long) stats.getMax() - columnPage.getShort(rowId);
            }
          case INT:
            if (stats.getDataType() == DataType.DOUBLE) {
              return (long) ((double) stats.getMax() - columnPage.getInt(rowId));
            } else {
              return (long) stats.getMax() - columnPage.getInt(rowId);
            }
          case LONG:
            if (stats.getDataType() == DataType.DOUBLE) {
              return (long) ((double) stats.getMax() - columnPage.getLong(rowId));
            } else {
              return (long) stats.getMax() - columnPage.getLong(rowId);
            }
        }
        break;
    }
    throw new RuntimeException("internal error while getting data: " + this);
  }

  @Override
  public double getDouble(int rowId) {
    switch (transform) {
      // page data is the result
      case NO_OP:
        switch (columnPage.getDataType()) {
          case BYTE:
            return columnPage.getByte(rowId);
          case SHORT:
            return columnPage.getShort(rowId);
          case INT:
            return columnPage.getInt(rowId);
          case LONG:
            return columnPage.getLong(rowId);

        }
        break;
      case MAX_DELTA:
        switch (columnPage.getDataType()) {
          // page data is diff from max
          case BYTE:
            return (double)stats.getMax() - columnPage.getByte(rowId);
          case SHORT:
            return (double)stats.getMax() - columnPage.getShort(rowId);
          case INT:
            return (double)stats.getMax() - columnPage.getInt(rowId);
          case LONG:
            return (double)stats.getMax() - columnPage.getLong(rowId);
        }
        break;
      case UPSCALE:
        double divisionFactor = Math.pow(10, stats.getDecimal());
        switch (columnPage.getDataType()) {
          case BYTE:
            return columnPage.getByte(rowId) / divisionFactor;
          case SHORT:
            return columnPage.getShort(rowId) / divisionFactor;
          case INT:
            return columnPage.getInt(rowId) / divisionFactor;
          case LONG:
            return columnPage.getLong(rowId) / divisionFactor;
          case FLOAT:
            return columnPage.getFloat(rowId) / divisionFactor;
          case DOUBLE:
            return columnPage.getDouble(rowId) / divisionFactor;
        }
        break;
      case UPSCALE_MAX_DELTA:
        double diff;
        divisionFactor = Math.pow(10, stats.getDecimal());
        switch (columnPage.getDataType()) {
          case BYTE:
            diff = columnPage.getByte(rowId) / divisionFactor;
            break;
          case SHORT:
            diff = columnPage.getShort(rowId) / divisionFactor;
            break;
          case INT:
            diff = columnPage.getInt(rowId) / divisionFactor;
            break;
          case LONG:
            diff = columnPage.getLong(rowId) / divisionFactor;
            break;
          case FLOAT:
            diff = columnPage.getFloat(rowId) / divisionFactor;
            break;
          case DOUBLE:
            diff = columnPage.getDouble(rowId) / divisionFactor;
            break;
          default:
            throw new RuntimeException("internal error while getting data: " + this);
        }
        return maxDecimal.subtract(BigDecimal.valueOf(diff)).doubleValue();
    }
    throw new RuntimeException("internal error while getting data: " + this);
  }
}
