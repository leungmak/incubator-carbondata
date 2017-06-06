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

package org.apache.carbondata.core.datastore.page.encoding;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for variable length data type (decimal, string).
 * This codec will flatten the variable length data before applying compression.
 */
public class VarLengthTypeCodec extends ColumnPageCodec {

  protected VarLengthTypeCodec(DataType srcDataType) {
    super(srcDataType, srcDataType, null);
  }

  public static VarLengthTypeCodec newInstance(DataType srcDataType) {
    return new VarLengthTypeCodec(srcDataType);
  }

  @Override
  public String getName() {
    return "VarLengthTypeCodec";
  }

  // it is a very basic encoder, just flatten the byte[][] to byte[] and apply compression
  @Override
  public byte[] encode(ColumnPage input) {
    switch (input.getDataType()) {
      case DECIMAL:
        return input.compress(compressor);
      case STRING:
        return input.compress(compressor);
      default:
        throw new UnsupportedOperationException("unsupported data type: " + input.getDataType());
    }
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) {
    return ColumnPage.decompress(compressor, targetDataType, input, offset, length);
  }
}
