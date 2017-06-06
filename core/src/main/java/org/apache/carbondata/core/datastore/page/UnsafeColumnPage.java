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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;

// This extension uses unsafe to store page data
public class UnsafeColumnPage extends ColumnPage {
  private MemoryBlock memoryBlock;

  // base address of memoryBlock
  private Object baseAddress;

  // base offset of memoryBlock
  private long baseOffset;

  // for variable length type: string and decimal
  // TODO: change to unsafe using int pointer
  private byte[][] byteArrayData;

  private final static int byteBits = DataType.BYTE.getSizeBits();
  private final static int shortBits = DataType.SHORT.getSizeBits();
  private final static int intBits = DataType.INT.getSizeBits();
  private final static int longBits = DataType.LONG.getSizeBits();
  private final static int floatBits = DataType.FLOAT.getSizeBits();
  private final static int doubleBits = DataType.DOUBLE.getSizeBits();

  UnsafeColumnPage(DataType dataType, int pageSize) throws MemoryException {
    super(dataType, pageSize);
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        int size = pageSize << dataType.getSizeBits();
        memoryBlock = UnsafeMemoryManager.allocateMemoryBlocking(size);
        baseAddress = memoryBlock.getBaseObject();
        baseOffset = memoryBlock.getBaseOffset();
        break;
      case DECIMAL:
      case STRING:
        byteArrayData = new byte[pageSize][];
        break;
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    long offset = rowId << byteBits;
    CarbonUnsafe.unsafe.putByte(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putShort(int rowId, short value) {
    long offset = rowId << shortBits;
    CarbonUnsafe.unsafe.putShort(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putInt(int rowId, int value) {
    long offset = rowId << intBits;
    CarbonUnsafe.unsafe.putInt(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putLong(int rowId, long value) {
    long offset = rowId << longBits;
    CarbonUnsafe.unsafe.putLong(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putDouble(int rowId, double value) {
    long offset = rowId << doubleBits;
    CarbonUnsafe.unsafe.putDouble(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putDecimalBytes(int rowId, byte[] decimalInBytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        decimalInBytes.length + CarbonCommonConstants.INT_SIZE_IN_BYTE);
    byteBuffer.putInt(decimalInBytes.length);
    byteBuffer.put(decimalInBytes);
    byteBuffer.flip();
    byteArrayData[rowId] = byteBuffer.array();
  }

  @Override
  public void putStringBytes(int rowId, byte[] stringInBytes) {
    byteArrayData[rowId] = stringInBytes;
  }

  @Override
  public byte getByte(int rowId) {
    long offset = rowId << byteBits;
    return CarbonUnsafe.unsafe.getByte(baseAddress, baseOffset + offset);
  }

  @Override
  public short getShort(int rowId) {
    long offset = rowId << shortBits;
    return CarbonUnsafe.unsafe.getShort(baseAddress, baseOffset + offset);
  }

  @Override
  public int getInt(int rowId) {
    long offset = rowId << intBits;
    return CarbonUnsafe.unsafe.getInt(baseAddress, baseOffset + offset);
  }

  @Override
  public long getLong(int rowId) {
    long offset = rowId << longBits;
    return CarbonUnsafe.unsafe.getLong(baseAddress, baseOffset + offset);
  }

  @Override
  public float getFloat(int rowId) {
    long offset = rowId << floatBits;
    return CarbonUnsafe.unsafe.getFloat(baseAddress, baseOffset + offset);
  }

  @Override
  public double getDouble(int rowId) {
    long offset = rowId << doubleBits;
    return CarbonUnsafe.unsafe.getDouble(baseAddress, baseOffset + offset);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    byte[] bytes = byteArrayData[rowId];
    return DataTypeUtil.byteToBigDecimal(bytes);
  }

  @Override
  public byte[] getBytePage() {
    byte[] data = new byte[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << byteBits;
      CarbonUnsafe.unsafe.getByte(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public short[] getShortPage() {
    short[] data = new short[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << shortBits;
      CarbonUnsafe.unsafe.getShort(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public int[] getIntPage() {
    int[] data = new int[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << intBits;
      CarbonUnsafe.unsafe.getInt(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public long[] getLongPage() {
    long[] data = new long[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << longBits;
      CarbonUnsafe.unsafe.getLong(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public float[] getFloatPage() {
    float[] data = new float[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << floatBits;
      CarbonUnsafe.unsafe.getFloat(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public double[] getDoublePage() {
    double[] data = new double[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << doubleBits;
      CarbonUnsafe.unsafe.getDouble(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[][] getDecimalPage() {
    return byteArrayData;
  }

  @Override
  public byte[][] getStringPage() {
    return byteArrayData;
  }

  @Override
  public void setBytePage(byte[] byteData) {
    for (int i = 0; i < byteData.length; i++) {
      long offset = i << byteBits;
      CarbonUnsafe.unsafe.putByte(baseAddress, baseOffset + offset, byteData[i]);
    }
  }

  @Override
  public void setShortPage(short[] shortData) {
    for (int i = 0; i < shortData.length; i++) {
      long offset = i << shortBits;
      CarbonUnsafe.unsafe.putShort(baseAddress, baseOffset + offset, shortData[i]);
    }
  }

  @Override
  public void setIntPage(int[] intData) {
    for (int i = 0; i < intData.length; i++) {
      long offset = i << intBits;
      CarbonUnsafe.unsafe.putInt(baseAddress, baseOffset + offset, intData[i]);
    }
  }

  @Override
  public void setLongPage(long[] longData) {
    for (int i = 0; i < longData.length; i++) {
      long offset = i << longBits;
      CarbonUnsafe.unsafe.putLong(baseAddress, baseOffset + offset, longData[i]);
    }
  }

  @Override
  public void setFloatPage(float[] floatData) {
    for (int i = 0; i < floatData.length; i++) {
      long offset = i << floatBits;
      CarbonUnsafe.unsafe.putFloat(baseAddress, baseOffset + offset, floatData[i]);
    }
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    for (int i = 0; i < doubleData.length; i++) {
      long offset = i << doubleBits;
      CarbonUnsafe.unsafe.putDouble(baseAddress, baseOffset + offset, doubleData[i]);
    }
  }

  @Override
  public void setDecimalPage(byte[][] decimalBytes) {
    this.byteArrayData = decimalBytes;
  }

  @Override
  public void setStringPage(byte[][] stringBytes) {
    this.byteArrayData = stringBytes;
  }

  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(memoryBlock);
    }
  }

  @Override
  protected void transformAndCastToByte(ColumnPageTransform transform, int param,
      DataType srcType) {
    assert (srcType.getSizeInBytes() >= BYTE.getSizeInBytes());
    if (srcType != BYTE) {
      byteData = new byte[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (srcType) {
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
            throw new UnsupportedOperationException("not support casting " + srcType + " to byte");
        }
        return;
      case MAX_DELTA:
        switch (srcType) {
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
        switch (srcType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              byteData[i] = (byte) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + srcType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (srcType) {
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
                srcType);
        }
        return;
    }
  }

  protected void transformAndCastToShort(ColumnPageTransform transform, int param, DataType srcType) {
    assert (srcType.getSizeInBytes() >= SHORT.getSizeInBytes());
    if (srcType != SHORT) {
      shortData = new short[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (srcType) {
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
            throw new UnsupportedOperationException("not support casting " + srcType + " to shot");
        }
        return;
      case MAX_DELTA:
        switch (srcType) {
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
        switch (srcType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              shortData[i] = (short) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + srcType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (srcType) {
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
                srcType);
        }
        return;
    }

  }

  protected void transformAndCastToInt(ColumnPageTransform transform, int param, DataType srcType) {
    assert (srcType.getSizeInBytes() >= INT.getSizeInBytes());
    if (srcType != INT) {
      intData = new int[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (srcType) {
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
            throw new UnsupportedOperationException("not support casting " + srcType + " to int");
        }
        return;
      case MAX_DELTA:
        switch (srcType) {
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
        switch (srcType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              intData[i] = (int) (Math.round(Math.pow(10, param) * doubleData[i]));
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + srcType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (srcType) {
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
                srcType);
        }
        return;
    }
  }

  protected void transformAndCastToLong(ColumnPageTransform transform, int param,
      DataType srcType) {
    assert (srcType.getSizeInBytes() >= LONG.getSizeInBytes());
    if (srcType != LONG) {
      longData = new long[pageSize];
    }
    switch (transform) {
      case NO_OP:
        switch (srcType) {
          case LONG:
            break;
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              longData[i] = (long) doubleData[i];
            }
            break;
          default:
            throw new UnsupportedOperationException("not support casting " + srcType + " to long");
        }
        return;
      case MAX_DELTA:
        switch (srcType) {
          case DOUBLE:
            double maxDouble = (double) stats.getMax();
            for (int i = 0; i < pageSize; i++) {
              longData[i] = (long) (maxDouble - doubleData[i]);
            }
            break;
        }
        return;
      case UPSCALE:
        switch (srcType) {
          case DOUBLE:
            for (int i = 0; i < pageSize; i++) {
              longData[i] = Math.round(Math.pow(10, param) * doubleData[i]);
            }
            break;
          default:
            throw new UnsupportedOperationException("not support UPSCALE for " + srcType);
        }
        return;
      case UPSCALE_MAX_DELTA:
        switch (srcType) {
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
                srcType);
        }
        return;
    }
  }

}