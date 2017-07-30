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
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;

public abstract class Decoder {
  private Compressor compressor;
  protected DataType dataType;

  public Decoder(Compressor compressor) {
    this.compressor = compressor;
  }

  public ColumnPage decode(byte[] input, int offset, int length, DataType srcDataType)
      throws IOException {
    this.dataType = srcDataType;
    initialize(srcDataType);
    switch (srcDataType) {
      case BYTE:
        byte[] bytes = compressor.unCompressByte(input, offset, length);
        return decodeByte(bytes);
      case SHORT:
        short[] shorts = compressor.unCompressShort(input, offset, length);
        return decodeShort(shorts);
      case INT:
        int[] ints = compressor.unCompressInt(input, offset, length);
        return decodeInt(ints);
      case LONG:
        long[] longs = compressor.unCompressLong(input, offset, length);
        return decodeLong(longs);
      default:
        throw new UnsupportedOperationException();
    }
  }

  abstract void initialize(DataType srcDataType);

  abstract ColumnPage decodeByte(byte[] input) throws IOException;

  abstract ColumnPage decodeShort(short[] input) throws IOException;

  abstract ColumnPage decodeInt(int[] input) throws IOException;

  abstract ColumnPage decodeLong(long[] input) throws IOException;

}