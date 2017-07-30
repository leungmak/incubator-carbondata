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

import org.apache.carbondata.core.metadata.datatype.DataType;

public class AdaptiveDecoderStream implements DecoderStream {

  private DataType targetDataType;
  private DecoderStream child;

  public AdaptiveDecoderStream(DataType targetDataType, DecoderStream child) {
    this.targetDataType = srcDataType;
    this.child = child;
  }

  @Override
  public void start(byte[] data, int offset, int length) {
    child.start(data, offset, length);
  }

  @Override
  public byte readByte() throws IOException {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public short readShort() throws IOException {
    assert targetDataType == DataType.BYTE;
    return child.readByte();
  }

  @Override public int readInt() throws IOException {
    switch (targetDataType) {
      case BYTE:
        return child.readByte();
      case SHORT:
        return child.readShort();
      default:
        throw new UnsupportedOperationException("internal error");
    }
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override public byte[] end() throws IOException {
    return new byte[0];
  }
}
