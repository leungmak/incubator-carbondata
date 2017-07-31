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
import org.apache.carbondata.core.metadata.encoder.CodecStreamMeta;
import org.apache.carbondata.core.metadata.encoder.DirectCompressCodecMeta;

public class DirectCompressEncoderStream implements EncoderStream {

  private Compressor compressor;
  private DataType dataType;
  private EncoderStream childStream;

  public DirectCompressEncoderStream(Compressor compressor, DataType dataType,
      EncoderStream childStream) {
    this.compressor = compressor;
    this.childStream = childStream;
    this.dataType = dataType;
  }

  @Override
  public void start() {
    childStream.start();
  }

  @Override
  public void write(byte value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(short value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(int value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(long value) throws IOException {
    childStream.write(value);
  }

  @Override
  public byte[] end() throws IOException {
    byte[] bytes = childStream.end();
    if (compressor != null) return compressor.compressByte(bytes);
    else return bytes;
  }

  @Override
  public CodecStreamMeta getMeta() {
    return new DirectCompressCodecMeta(compressor.getName(), dataType);
  }
}
