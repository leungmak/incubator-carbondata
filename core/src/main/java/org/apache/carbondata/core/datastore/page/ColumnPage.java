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
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL;
import static org.apache.carbondata.core.metadata.datatype.DataType.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataType.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;
import static org.apache.carbondata.core.metadata.datatype.DataType.STRING;

/**
 * Represent a columnar data in one page for one column.
 */
public class ColumnPage implements CarbonReadDataHolder {

  private final int pageSize;
  private DataType dataType;
  private ColumnPageStatsVO stats;

  // Only one of following fields will be used
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;

  // for string and decimal data
  private byte[][] byteArrayData;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  private final static boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_LOADING_UNSAFE_COLUMN_PAGE,
          CarbonCommonConstants.ENABLE_LOADING_UNSAFE_COLUMN_PAGE_DEFAULT));

  protected ColumnPage(DataType dataType, int pageSize) {
    this.pageSize = pageSize;
    this.dataType = dataType;
    this.stats = new ColumnPageStatsVO(dataType);
    this.nullBitSet = new BitSet(pageSize);
  }

  private static ColumnPage createPage(DataType dataType, int pageSize) {
    if (unsafe) {
      try {
        return new UnsafeColumnPage(dataType, pageSize);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new ColumnPage(dataType, pageSize);
    }
  }

  // create a new page
  public static ColumnPage newPage(DataType dataType, int pageSize) {
    ColumnPage instance;
    switch (dataType) {
      case BYTE:
        instance = newBytePage(new byte[pageSize]);
        break;
      case SHORT:
        instance = newShortPage(new short[pageSize]);
        break;
      case INT:
        instance = newIntPage(new int[pageSize]);
        break;
      case LONG:
        instance = newLongPage(new long[pageSize]);
        break;
      case FLOAT:
        instance = newFloatPage(new float[pageSize]);
        break;
      case DOUBLE:
        instance = newDoublePage(new double[pageSize]);
        break;
      case DECIMAL:
        instance = newDecimalPage(new byte[pageSize][]);
        break;
      case STRING:
        instance = newStringPage(new byte[pageSize][]);
        break;
      default:
        throw new RuntimeException("Unsupported data dataType: " + dataType);
    }
    return instance;
  }

  private static ColumnPage newBytePage(byte[] byteData) {
    ColumnPage columnPage = createPage(BYTE, byteData.length);
    columnPage.setBytePage(byteData);
    return columnPage;
  }

  private static ColumnPage newShortPage(short[] shortData) {
    ColumnPage columnPage = createPage(SHORT, shortData.length);
    columnPage.setShortPage(shortData);
    return columnPage;
  }

  private static ColumnPage newIntPage(int[] intData) {
    ColumnPage columnPage = createPage(INT, intData.length);
    columnPage.setIntPage(intData);
    return columnPage;
  }

  private static ColumnPage newLongPage(long[] longData) {
    ColumnPage columnPage = createPage(LONG, longData.length);
    columnPage.setLongPage(longData);
    return columnPage;
  }

  private static ColumnPage newFloatPage(float[] floatData) {
    ColumnPage columnPage = createPage(FLOAT, floatData.length);
    columnPage.setFloatPage(floatData);
    return columnPage;
  }

  private static ColumnPage newDoublePage(double[] doubleData) {
    ColumnPage columnPage = createPage(DOUBLE, doubleData.length);
    columnPage.setDoublePage(doubleData);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(byte[][] decimalData) {
    ColumnPage columnPage = createPage(DECIMAL, decimalData.length);
    columnPage.setDecimalPage(decimalData);
    return columnPage;
  }

  private static ColumnPage newStringPage(byte[][] stringData) {
    ColumnPage columnPage = new ColumnPage(STRING, stringData.length);
    columnPage.setStringPage(stringData);
    return columnPage;
  }

  public DataType getDataType() {
    return dataType;
  }

  public ColumnPageStatsVO getStatistics() {
    return stats;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void putData(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
      stats.updateNull();
      return;
    }
    switch (dataType) {
      case BYTE:
        // TODO: change sort step to store as exact data type
        putByte(rowId, ((Long) value).byteValue());
        break;
      case SHORT:
        putShort(rowId, (short) value);
        break;
      case INT:
        putInt(rowId, (int) value);
        break;
      case LONG:
        putLong(rowId, (long) value);
        break;
      case DOUBLE:
        putDouble(rowId, (double) value);
        break;
      case DECIMAL:
        putDecimalBytes(rowId, (byte[]) value);
        break;
      case STRING:
        putStringBytes(rowId, (byte[]) value);
        break;
      default:
        throw new RuntimeException("unsupported data type: " + dataType);
    }
    stats.update(value);
  }

  /**
   * Set byte value at rowId
   */
  public void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  /**
   * Set short value at rowId
   */
  public void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  /**
   * Set integer value at rowId
   */
  public void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  /**
   * Set long value at rowId
   */
  public void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  /**
   * Set double value at rowId
   */
  public void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  /**
   * Set decimal value at rowId
   */
  public void putDecimalBytes(int rowId, byte[] decimalInBytes) {
    // do LV (length value) coded of input bytes
    ByteBuffer byteBuffer = ByteBuffer.allocate(decimalInBytes.length +
        CarbonCommonConstants.INT_SIZE_IN_BYTE);
    byteBuffer.putInt(decimalInBytes.length);
    byteBuffer.put(decimalInBytes);
    byteBuffer.flip();
    byteArrayData[rowId] = byteBuffer.array();
  }

  /**
   * Set string value at rowId
   */
  public void putStringBytes(int rowId, byte[] stringInBytes) {
    byteArrayData[rowId] = stringInBytes;
  }

  /**
   * Set null at rowId
   */
  public void putNull(int rowId) {
    nullBitSet.set(rowId);
    switch (dataType) {
      case BYTE:
        putByte(rowId, (byte) 0);
        break;
      case SHORT:
        putShort(rowId, (short) 0);
        break;
      case INT:
        putInt(rowId, 0);
        break;
      case LONG:
        putLong(rowId, 0L);
        break;
      case DOUBLE:
        putDouble(rowId, 0.0);
        break;
      case DECIMAL:
        byte[] decimalInBytes = DataTypeUtil.bigDecimalToByte(BigDecimal.ZERO);
        putDecimalBytes(rowId, decimalInBytes);
        break;
    }
  }

  /**
   * Get byte value at rowId
   */
  public byte getByte(int rowId) {
    return byteData[rowId];
  }

  /**
   * Get short value at rowId
   */
  public short getShort(int rowId) {
    return shortData[rowId];
  }

  /**
   * Get int value at rowId
   */
  public int getInt(int rowId) {
    return intData[rowId];
  }

  /**
   * Get long value at rowId
   */
  public long getLong(int rowId) {
    return longData[rowId];
  }

  /**
   * Get float value at rowId
   */
  public float getFloat(int rowId) {
    return floatData[rowId];
  }

  /**
   * Get double value at rowId
   */
  public double getDouble(int rowId) {
    return doubleData[rowId];
  }

  public BigDecimal getDecimal(int rowId) {
    byte[] bytes = byteArrayData[rowId];
    return DataTypeUtil.byteToBigDecimal(bytes);
  }

  /**
   * Get byte value page
   */
  public byte[] getBytePage() {
    return byteData;
  }

  /**
   * Get short value page
   */
  public short[] getShortPage() {
    return shortData;
  }

  /**
   * Get int value page
   */
  public int[] getIntPage() {
    return intData;
  }

  /**
   * Get long value page
   */
  public long[] getLongPage() {
    return longData;
  }

  /**
   * Get float value page
   */
  public float[] getFloatPage() {
    return floatData;
  }

  /**
   * Get double value page
   */
  public double[] getDoublePage() {
    return doubleData;
  }

  /**
   * Get decimal value page
   */
  public byte[][] getDecimalPage() {
    return byteArrayData;
  }

  /**
   * Get string page
   */
  public byte[][] getStringPage() {
    return byteArrayData;
  }

  /**
   * Get null bitset page
   */
  public BitSet getNullBitSet() {
    return nullBitSet;
  }

  /**
   * Set byte values to page
   */
  public void setBytePage(byte[] byteData) {
    this.byteData = byteData;
  }

  /**
   * Set short values to page
   */
  public void setShortPage(short[] shortData) {
    this.shortData = shortData;
  }

  /**
   * Set int values to page
   */
  public void setIntPage(int[] intData) {
    this.intData = intData;
  }

  /**
   * Set long values to page
   */
  public void setLongPage(long[] longData) {
    this.longData = longData;
  }

  /**
   * Set float values to page
   */
  public void setFloatPage(float[] floatData) {
    this.floatData = floatData;
  }

  /**
   * Set double value to page
   */
  public void setDoublePage(double[] doubleData) {
    this.doubleData = doubleData;
  }

  /**
   * Set decimal values to page
   */
  public void setDecimalPage(byte[][] decimalBytes) {
    this.byteArrayData = decimalBytes;
  }

  /**
   * Set string values to page
   */
  public void setStringPage(byte[][] stringBytes) {
    this.byteArrayData = stringBytes;
  }

  public void freeMemory() {
  }

  /**
   * apply transformation to page data and cast data type to specified target data type
   * @param transform type of transformation
   * @param param parameter of the transformation
   * @param targetDataType data type to cast to
   */
  public void transformAndCastTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    switch (dataType) {
      case SHORT:
        transformAndCastShortTo(transform, param, targetDataType);
        break;
      case INT:
        transformAndCastIntTo(transform, param, targetDataType);
        break;
      case LONG:
        transformAndCastLongTo(transform, param, targetDataType);
        break;
      case FLOAT:
        transformAndCastFloatTo(transform, param, targetDataType);
        break;
      case DOUBLE:
        transformAndCastDoubleTo(transform, param, targetDataType);
        break;
      default:
        throw new UnsupportedOperationException("not support cast column page from " + dataType +
            " to " + targetDataType);
    }
    dataType = targetDataType;
  }

  protected void transformAndCastToByte(ColumnPageTransform transform, int param) {
    assert (dataType.getSizeInBytes() >= BYTE.getSizeInBytes());
    if (dataType != BYTE) {
      byteData = new byte[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (dataType) {
          case BYTE:
            break;
          case SHORT:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) shortData[i];
            }
            break;
          case INT:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) intData[i];
            }
            break;
          case LONG:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) longData[i];
            }
            break;
          case FLOAT:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) floatData[i];
            }
            break;
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) doubleData[i];
            }
            break;
          default:
            throw new UnsupportedOperationException("not support casting " + dataType + " to byte");
        }
        return;
      case MAX_DELTA:
        switch (dataType) {
          case SHORT:
            long maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (maxLong  - shortData[i]);
            }
            break;
          case INT:
            maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (maxLong  - intData[i]);
            }
            break;
          case LONG:
            maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (maxLong  - longData[i]);
            }
            break;
          case FLOAT:
            float maxFloat = (float) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (maxFloat - floatData[i]);
            }
            break;
          case DOUBLE:
            double maxDouble = (double) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (maxDouble - doubleData[i]);
            }
            break;
        }
        return;
      case UPSCALE:
        switch (dataType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + dataType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (dataType) {
          case DOUBLE:
            BigDecimal max = BigDecimal.valueOf((double)stats.getMax());
            for (int i = 0; i < pageSize; i++) {
              BigDecimal val = BigDecimal.valueOf(doubleData[i]);
              double diff = max.subtract(val).doubleValue();
              byteData[i] = (byte) (Math.round(diff * Math.pow(10, param)));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE_MAX_DELTA for " +
                dataType);
        }
        return;
    }
  }

  protected void transformAndCastToShort(ColumnPageTransform transform, int param) {
    assert (dataType.getSizeInBytes() >= SHORT.getSizeInBytes());
    if (dataType != SHORT) {
      shortData = new short[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (dataType) {
          case SHORT:
            break;

          case INT:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) intData[i];
            }
            break;
          case LONG:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) longData[i];
            }
            break;
          case FLOAT:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) floatData[i];
            }
            break;
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) doubleData[i];
            }
            break;
          default:
            throw new UnsupportedOperationException("not support casting " + dataType + " to shot");
        }
        return;
      case MAX_DELTA:
        switch (dataType) {
          case INT:
            long maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (maxLong  - intData[i]);
            }
            break;
          case LONG:
            maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (maxLong  - longData[i]);
            }
            break;
          case FLOAT:
            float maxFloat = (float) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (maxFloat - floatData[i]);
            }
            break;
          case DOUBLE:
            double maxDouble = (double) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (maxDouble - doubleData[i]);
            }
            break;
        }
        return;
      case UPSCALE:
        switch (dataType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + dataType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (dataType) {
          case DOUBLE:
            BigDecimal max = BigDecimal.valueOf((double)stats.getMax());
            for (int i = 0; i < pageSize; i++) {
              BigDecimal val = BigDecimal.valueOf(doubleData[i]);
              double diff = max.subtract(val).doubleValue();
              shortData[i] = (short) (Math.round(diff * Math.pow(10, param)));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE_MAX_DELTA for " +
                dataType);
        }
        return;
    }

  }

  protected void transformAndCastToInt(ColumnPageTransform transform, int param) {
    assert (dataType.getSizeInBytes() >= INT.getSizeInBytes());
    if (dataType != INT) {
      intData = new int[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (dataType) {
          case INT:
            break;
          case LONG:
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) longData[i];
            }
            break;
          case FLOAT:
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) floatData[i];
            }
            break;
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) doubleData[i];
            }
            break;
          default:
            throw new UnsupportedOperationException("not support casting " + dataType + " to int");
        }
        return;
      case MAX_DELTA:
        switch (dataType) {
          case LONG:
            long maxLong = (long) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) (maxLong  - longData[i]);
            }
            break;
          case FLOAT:
            float maxFloat = (float) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) (maxFloat - floatData[i]);
            }
            break;
          case DOUBLE:
            double maxDouble = (double) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) (maxDouble - doubleData[i]);
            }
            break;
        }
        return;
      case UPSCALE:
        switch (dataType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + dataType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (dataType) {
          case DOUBLE:
            BigDecimal max = BigDecimal.valueOf((double)stats.getMax());
            for (int i = 0; i < pageSize; i++) {
              BigDecimal val = BigDecimal.valueOf(doubleData[i]);
              double diff = max.subtract(val).doubleValue();
              intData[i] = (int) (Math.round(diff * Math.pow(10, param)));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE_MAX_DELTA for " +
                dataType);
        }
        return;
    }
  }

  protected void transformAndCastToLong(ColumnPageTransform transform, int param) {
    assert (dataType.getSizeInBytes() >= LONG.getSizeInBytes());
    if (dataType != LONG) {
      longData = new long[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (dataType) {
          case LONG:
            break;
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              longData[i] = (long) doubleData[i];
            }
            break;
          default:
            throw new UnsupportedOperationException("not support casting " + dataType + " to long");
        }
        return;
      case MAX_DELTA:
        switch (dataType) {
          case DOUBLE:
            double maxDouble = (double) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              longData[i] = (long) (maxDouble - doubleData[i]);
            }
            break;
        }
        return;
      case UPSCALE:
        switch (dataType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              longData[i] = Math.round(Math.pow(10, param) * doubleData[i]);
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + dataType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (dataType) {
          case DOUBLE:
            BigDecimal max = BigDecimal.valueOf((double)stats.getMax());
            for (int i = 0; i < pageSize; i++) {
              BigDecimal val = BigDecimal.valueOf(doubleData[i]);
              double diff = max.subtract(val).doubleValue();
              longData[i] = Math.round(diff * Math.pow(10, param));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE_MAX_DELTA for " +
                dataType);
        }
        return;
    }
  }

  /**
   * cast short page to target data type, target data type scope should be smaller than short
   */
  private void transformAndCastShortTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    assert (targetDataType.getSizeInBytes() <= SHORT.getSizeInBytes());
    switch (targetDataType) {
      case BYTE:
        transformAndCastToByte(transform, param);
        break;
      case SHORT:
        transformAndCastToShort(transform, param);
        return;
      default:
        throw new UnsupportedOperationException("not support " + transform +
            " and casting short to " + targetDataType);
    }
    shortData = null;
  }

  /**
   * cast int page to target data type, target data type scope should be smaller than int
   */
  private void transformAndCastIntTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    assert (targetDataType.getSizeInBytes() <= INT.getSizeInBytes());
    switch (targetDataType) {
      case BYTE:
        transformAndCastToByte(transform, param);
        break;
      case SHORT:
        transformAndCastToShort(transform, param);
        break;
      case INT:
        transformAndCastToInt(transform, param);
        return;
      default:
        throw new UnsupportedOperationException("not support " + transform +
            " and casting int to " + targetDataType);
    }
    intData = null;
  }

  /**
   * cast long page to target data type, target data type scope should be smaller than long
   */
  private void transformAndCastLongTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    assert (targetDataType.getSizeInBytes() <= LONG.getSizeInBytes());
    switch (targetDataType) {
      case BYTE:
        transformAndCastToByte(transform, param);
        break;
      case SHORT:
        transformAndCastToShort(transform, param);
        break;
      case INT:
        transformAndCastToInt(transform, param);
        break;
      case LONG:
        transformAndCastToLong(transform, param);
        return;
      default:
        throw new UnsupportedOperationException("not support " + transform +
            " and casting int to " + targetDataType);
    }
    longData = null;
  }

  /**
   * cast double page to target data type, target data type scope should be smaller than double
   * (target can also be non-floating point type)
   */
  private void transformAndCastDoubleTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    assert (targetDataType.getSizeInBytes() <= DOUBLE.getSizeInBytes());
    switch (targetDataType) {
      case BYTE:
        transformAndCastToByte(transform, param);
        break;
      case SHORT:
        transformAndCastToShort(transform, param);
        break;
      case INT:
        transformAndCastToInt(transform, param);
        break;
      case LONG:
        transformAndCastToLong(transform, param);
        break;
      case DOUBLE:
        return;
      default:
        throw new UnsupportedOperationException("not support " + transform +
            " and casting double to " + targetDataType);
    }
    doubleData = null;
  }

  /**
   * cast float page to target data type, target data type scope should be smaller than float
   * (target can also be non-floating point type)
   */
  private void transformAndCastFloatTo(ColumnPageTransform transform, int param,
      DataType targetDataType) {
    assert (targetDataType.getSizeInBytes() <= FLOAT.getSizeInBytes());
    switch (targetDataType) {
      case BYTE:
        transformAndCastToByte(transform, param);
        break;
      case SHORT:
        transformAndCastToShort(transform, param);
        break;
      case INT:
        transformAndCastToInt(transform, param);
        break;
      case LONG:
        transformAndCastToLong(transform, param);
        break;
      case FLOAT:
        return;
      default:
        throw new UnsupportedOperationException("not support " + transform +
            " and casting float to " + targetDataType);
    }
    floatData = null;
  }

  /**
   * compress page data using specified compressor
   */
  public byte[] compress(Compressor compressor) {
    switch (dataType) {
      case BYTE:
        return compressor.compressByte(getBytePage());
      case SHORT:
        return compressor.compressShort(getShortPage());
      case INT:
        return compressor.compressInt(getIntPage());
      case LONG:
        return compressor.compressLong(getLongPage());
      case FLOAT:
        return compressor.compressFloat(getFloatPage());
      case DOUBLE:
        return compressor.compressDouble(getDoublePage());
      case DECIMAL:
        byte[] flattenedDecimal = ByteUtil.flatten(getDecimalPage());
        return compressor.compressByte(flattenedDecimal);
      case STRING:
        byte[] flattenedString = ByteUtil.flatten(getStringPage());
        return compressor.compressByte(flattenedString);
      default:
        throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  /**
   * decompress data and create a column page using the decompressed data
   */
  public static ColumnPage decompress(Compressor compressor, DataType dataType,
      byte[] compressedData, int offset, int length) {
    switch (dataType) {
      case BYTE:
        byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
        return newBytePage(byteData);
      case SHORT:
        short[] shortData = compressor.unCompressShort(compressedData, offset, length);
        return newShortPage(shortData);
      case INT:
        int[] intData = compressor.unCompressInt(compressedData, offset, length);
        return newIntPage(intData);
      case LONG:
        long[] longData = compressor.unCompressLong(compressedData, offset, length);
        return newLongPage(longData);
      case FLOAT:
        float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
        return newFloatPage(floatData);
      case DOUBLE:
        double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
        return newDoublePage(doubleData);
      case DECIMAL:
        byte[] decompressed = compressor.unCompressByte(compressedData, offset, length);
        byte[][] decimal = deflatten(decompressed);
        return newDecimalPage(decimal);
      case STRING:
        decompressed = compressor.unCompressByte(compressedData, offset, length);
        byte[][] string = deflatten(decompressed);
        return newStringPage(string);
      default:
        throw new UnsupportedOperationException("unsupport uncompress column page: " + dataType);
    }
  }

  // input byte[] is LV encoded, this function can expand it into byte[][]
  private static byte[][] deflatten(byte[] input) {
    int pageSize = Integer.valueOf(
        CarbonProperties.getInstance().getProperty(
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT));
    int numRows = 0;
    // offset of value of each row in input data
    int[] offsetOfRow = new int[pageSize];
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    for (int currentLength = 0; currentLength < input.length;) {
      buffer.put(input, currentLength, CarbonCommonConstants.INT_SIZE_IN_BYTE);
      buffer.flip();
      int valueLength = buffer.getInt();
      offsetOfRow[numRows] = currentLength + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      currentLength += CarbonCommonConstants.INT_SIZE_IN_BYTE + valueLength;
      buffer.clear();
      numRows++;
    }
    byte[][] byteArrayData = new byte[numRows][];
    for (int rowId = 0; rowId < numRows; rowId++) {
      int valueOffset = offsetOfRow[rowId];
      int valueLength;
      if (rowId != numRows - 1) {
        valueLength = offsetOfRow[rowId + 1] - valueOffset - CarbonCommonConstants.INT_SIZE_IN_BYTE;
      } else {
        // last row
        buffer.put(input, offsetOfRow[rowId] - CarbonCommonConstants.INT_SIZE_IN_BYTE,
            CarbonCommonConstants.INT_SIZE_IN_BYTE);
        buffer.flip();
        valueLength = buffer.getInt();
      }
      byte[] value = new byte[valueLength];
      System.arraycopy(input, valueOffset, value, 0, valueLength);
      byteArrayData[rowId] = value;
    }
    return byteArrayData;
  }
}
