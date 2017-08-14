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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Encoded measure page that include data and statistics
 */
public class EncodedMeasurePage {

  /**
   * Constructor
   * @param pageSize number of row of the encoded page
   * @param encodedData encoded data for this page
   * @param nullBitSet null bit set (1 indicates null at rowId)
   * @param encodings encoding type used for this page
   * @param metaDatas metadata of the encoding
   */
  /*
  public EncodedMeasurePage(int pageSize, byte[] encodedData, BitSet nullBitSet,
      List<Encoding> encodings, List<ValueEncoderMeta> metaDatas) {
    super(pageSize, encodedData, nullBitSet, encodings, metaDatas);
  }

  @Override
  BlockletMinMaxIndex buildMinMaxIndex() {
    BlockletMinMaxIndex minMaxIndex = new BlockletMinMaxIndex();
    assert (metaDatas.size() == 1);
    ValueEncoderMeta metaData = metaDatas.get(0);
    if (metaData instanceof ColumnPageEncoderMeta) {
      ColumnPageEncoderMeta meta = (ColumnPageEncoderMeta) metaData;
      minMaxIndex.addToMax_values(ByteBuffer.wrap(meta.getMaxAsBytes()));
      minMaxIndex.addToMin_values(ByteBuffer.wrap(meta.getMinAsBytes()));
    } else {
      // for backward compatibility
      minMaxIndex.addToMax_values(ByteBuffer.wrap(CarbonUtil.getMaxValueAsBytes(metaData)));
      minMaxIndex.addToMin_values(ByteBuffer.wrap(CarbonUtil.getMinValueAsBytes(metaData)));
    }
    return minMaxIndex;
  }

  @Override
  List<ByteBuffer> buildEncoderMeta() throws IOException {
    assert (metaDatas.size() == 1);
    assert (encodings.size() == 1);
    Encoding encoding = encodings.get(0);
    ValueEncoderMeta metaData = metaDatas.get(0);
    List<ByteBuffer> encoderMetaList = new ArrayList<ByteBuffer>();
    if (metaData instanceof ColumnPageEncoderMeta) {
      ColumnPageEncoderMeta meta = (ColumnPageEncoderMeta) metaData;
      ByteBuffer buffer = serializeEncodingMetaData(encoding, meta);
      encoderMetaList.add(buffer);
    } else {
      // for backward compatibility
      encoderMetaList.add(ByteBuffer.wrap(CarbonUtil.serializeEncodeMetaUsingByteBuffer(metaData)));
    }
    return encoderMetaList;
  }

  @Override
  void fillDimensionFields(DataChunk2 dataChunk) {
  }

  @Override
  public int getTotalSerializedSize() throws IOException {
    int metadataSize = CarbonUtil.getByteArray(getPageHeader()).length;
    int dataSize = encodedData.length;
    return dataSize + metadataSize;
  }

  public ValueEncoderMeta getMetaData() {
    assert (metaDatas.size() == 1);
    return metaDatas.get(0);
  }
  */
}