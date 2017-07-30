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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;

public class ByteArrayEncoderStream implements EncoderStream {

  private ByteArrayOutputStream bao;
  private DataOutputStream outputStream;
  public ByteArrayEncoderStream() {
    bao = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(bao);
  }

  @Override
  public void start() {

  }

  @Override
  public void write(byte value) throws IOException {
    outputStream.writeByte(value);
  }

  @Override
  public void write(short value) throws IOException {
    outputStream.writeShort(value);
  }

  @Override
  public void write(int value) throws IOException {
    outputStream.writeInt(value);
  }

  @Override
  public void write(long value) throws IOException {
    outputStream.writeLong(value);
  }

  @Override
  public byte[] end() throws IOException {
    return bao.toByteArray();
  }

  @Override
  public List<ValueEncoderMeta> getMeta() {
    return null;
  }
}
