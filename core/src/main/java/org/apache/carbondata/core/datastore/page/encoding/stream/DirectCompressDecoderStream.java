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

package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class DirectCompressDecoderStream implements DecoderStream {
  private Compressor compressor;
  private DataType dataType;
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;
  private int currentRow;

  public DirectCompressDecoderStream(Compressor compressor, DataType dataType) {
    this.compressor = compressor;
    this.dataType = dataType;
  }

  @Override
  public void start(byte[] data, int offset, int length) {
    switch (dataType) {
      case BYTE:
        byteData = compressor.unCompressByte(data, offset, length);
        break;
      case SHORT:
        shortData = compressor.unCompressShort(data, offset, length);
        break;
      case SHORT_INT:
        intData = compressor.unCompressInt(data, offset, length);
        break;
      case INT:
        intData = compressor.unCompressInt(data, offset, length);
        break;
      case LONG:
        longData = compressor.unCompressLong(data, offset, length);
        break;
      case FLOAT:
        floatData = compressor.unCompressFloat(data, offset, length);
        break;
      case DOUBLE:
        doubleData = compressor.unCompressDouble(data, offset, length);
        break;
      default:
        throw new UnsupportedOperationException("unsupported: " + dataType);
    }
  }

  @Override
  public byte readByte() throws IOException {
    return byteData[currentRow++];
  }

  @Override
  public short readShort() throws IOException {
    return shortData[currentRow++];
  }

  @Override
  public int readInt() throws IOException {
    return intData[currentRow++];
  }

  @Override
  public long readLong() throws IOException {
    return longData[currentRow++];
  }

  @Override
  public void end() throws IOException {
  }

}
