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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.CodecMetaFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class AdaptiveCodecMeta extends CodecStreamMeta {

  private DataType srcDataType;
  private DataType targetDataType;

  public AdaptiveCodecMeta(DataType srcDataType, DataType targetDataType) {
    this.srcDataType = srcDataType;
    this.targetDataType = targetDataType;
  }

  public DataType getSrcDataType() {
    return srcDataType;
  }

  public DataType getTargetDataType() {
    return targetDataType;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(srcDataType.ordinal());
    out.writeInt(targetDataType.ordinal());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.srcDataType = DataType.valueOf(in.readInt());
    this.targetDataType = DataType.valueOf(in.readInt());
  }
}
