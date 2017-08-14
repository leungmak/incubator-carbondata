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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.DimensionType;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressorEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLEEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * An column page after encoding.
 */
public class EncodedColumnPage {

  // encoded and compressed column page data
  protected final byte[] encodedData;

  // null bit set, 1 indicates null for rowId (position of the bit in the bitset)
  protected final BitSet nullBitSet;

  // metadata of this page
  private DataChunk2 pageHeader;

  // stats of this page
  private SimpleStatsResult stats;

  /**
   * Constructor
   * @param pageHeader metadata of the encoded page
   * @param encodedData encoded data for this page
   * @param nullBitSet null bit set (1 indicates null at rowId)
   */
  public EncodedColumnPage(DataChunk2 pageHeader, byte[] encodedData, BitSet nullBitSet,
      SimpleStatsResult stats) {
    if (pageHeader == null) {
      throw new IllegalArgumentException("data chunk2 must not be null");
    }
    if (encodedData == null) {
      throw new IllegalArgumentException("encoded data must not be null");
    }
    if (nullBitSet == null) {
      throw new IllegalArgumentException("null bit set must not be null");
    }
    this.pageHeader = pageHeader;
    this.encodedData = encodedData;
    this.nullBitSet = nullBitSet;
    this.stats = stats;
  }

  /**
   * return the encoded data as ByteBuffer
   */
  public ByteBuffer getEncodedData() {
    return ByteBuffer.wrap(encodedData);
  }

  public DataChunk2 getPageHeader() {
    return pageHeader;
  }

  public BitSet getNullBitSet() {
    return nullBitSet;
  }

  /**
   * Return the total size of serialized data and metadata
   */
  public int getTotalSerializedSize() {
    int metadataSize = CarbonUtil.getByteArray(pageHeader).length;
    int dataSize = encodedData.length;
    return metadataSize + dataSize;
    /* for legacy dimension
        int size = encodedData.length;
    if (indexStorage != null) {
      if (indexStorage.getRowIdPageLengthInBytes() > 0) {
        size += getTotalRowIdPageLengthInBytes();
      }
      if (indexStorage.getDataRlePageLengthInBytes() > 0) {
        size += indexStorage.getDataRlePageLengthInBytes();
      }
    }
     */
  }

  protected ByteBuffer serializeEncodingMetaData(Encoding encoding, ColumnPageEncoderMeta meta)
      throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    switch (encoding) {
      case DIRECT_COMPRESS:
        DirectCompressorEncoderMeta directCompressorCodecMeta = (DirectCompressorEncoderMeta) meta;
        directCompressorCodecMeta.write(out);
        break;
      case ADAPTIVE_INTEGRAL:
        AdaptiveIntegralEncoderMeta adaptiveCodecMeta = (AdaptiveIntegralEncoderMeta) meta;
        adaptiveCodecMeta.write(out);
        break;
      case ADAPTIVE_DELTA_INTEGRAL:
        AdaptiveDeltaIntegralEncoderMeta deltaCodecMeta = (AdaptiveDeltaIntegralEncoderMeta) meta;
        deltaCodecMeta.write(out);
        break;
      case RLE_INTEGRAL:
        RLEEncoderMeta rleCodecMeta = (RLEEncoderMeta) meta;
        rleCodecMeta.write(out);
        break;
      default:
        throw new UnsupportedOperationException("unknown encoding: " + encoding);
    }
    return ByteBuffer.wrap(stream.toByteArray());
  }

  public IndexStorage getIndexStorage() {
    throw new UnsupportedOperationException("internal error");
  }

  public DimensionType getDimensionType() {
    throw new UnsupportedOperationException("internal error");
  }

  public SimpleStatsResult getStats() {
    return stats;
  }
}