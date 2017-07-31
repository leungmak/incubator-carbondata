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

package org.apache.carbondata.core.metadata.encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.carbondata.core.datastore.page.encoding.stream.DecoderStream;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.schema.table.Writable;

/**
 * It holds metadata for one column page
 */
public class ColumnPageCodecMeta extends ValueEncoderMeta implements Writable {

  private Stack<CodecStreamMeta> metas;

  public ColumnPageCodecMeta() {
    this.metas = new Stack<>();
  }

  public void addEncoding(CodecStreamMeta meta) {
    this.metas.push(meta);
  }

  public List<CodecStreamMeta> getEncodingList() {
    return metas;
  }

  DecoderStream createDecoder(byte[])

  @Override
  public void write(DataOutput out) throws IOException {
    int size = metas.size();
    out.writeInt(size);
    for (int i = size - 1; i >= 0; i--) {
      CodecStreamMeta meta = metas.get(i);
      meta.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      CodecStreamMeta meta = new CodecStreamMeta();
      meta.readFields(in);
      metas.push(meta);
    }
  }
}
