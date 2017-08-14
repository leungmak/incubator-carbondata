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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DimensionType;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.datastore.page.statistics.TablePageStatistics;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.SortState;

/**
 * Encoded dimension page that include data and inverted index
 */
public class EncodedDimensionPage extends EncodedColumnPage {
  private IndexStorage indexStorage;
  private DimensionType dimensionType;

  /**
   * Constructor
   *
   * @param pageHeader  metadata of the encoded page
   * @param encodedData encoded data for this page
   * @param nullBitSet  null bit set (1 indicates null at rowId)
   */
  EncodedDimensionPage(DataChunk2 pageHeader, byte[] encodedData, BitSet nullBitSet,
      SimpleStatsResult stats, IndexStorage indexStorage, DimensionType dimensionType) {
    super(pageHeader, encodedData, nullBitSet, stats);
    this.indexStorage = indexStorage;
    this.dimensionType = dimensionType;
  }

  @Override
  public IndexStorage getIndexStorage() {
    return indexStorage;
  }

  @Override
  public DimensionType getDimensionType() {
    return dimensionType;
  }

  /**
   * Constructor
   * @param pageSize number of row of the encoded page
   * @param encodedData encoded data for this page
   * @param nullBitSet null bit set (1 indicates null at rowId)
   * @param encodings encoding type used for this page
   * @param metaDatas metadata of the encoding
   * @param indexStorage //TODO: need to refactor, set in metadata
   */
  /*
  public EncodedDimensionPage(int pageSize, byte[] encodedData, BitSet nullBitSet,
      List<Encoding> encodings, List<ValueEncoderMeta> metaDatas, DimensionType dimensionType,
      IndexStorage indexStorage) {
    super(pageSize, encodedData, nullBitSet, encodings, metaDatas);
    this.dimensionType = dimensionType;
    this.indexStorage = indexStorage;
  }

  private int getTotalRowIdPageLengthInBytes() {
    return CarbonCommonConstants.INT_SIZE_IN_BYTE +
        indexStorage.getRowIdPageLengthInBytes() + indexStorage.getRowIdRlePageLengthInBytes();
  }

  private int getSerializedDataSize() {
    int size = encodedData.length;
    if (indexStorage != null) {
      if (indexStorage.getRowIdPageLengthInBytes() > 0) {
        size += getTotalRowIdPageLengthInBytes();
      }
      if (indexStorage.getDataRlePageLengthInBytes() > 0) {
        size += indexStorage.getDataRlePageLengthInBytes();
      }
    }
    return size;
  }

  @Override
  public int getTotalSerializedSize() throws IOException {
    int metadataSize = CarbonUtil.getByteArray(getPageHeader()).length;

    int dataSize = 0;
    if (indexStorage != null) {
      if (!indexStorage.isAlreadySorted()) {
        dataSize += indexStorage.getRowIdPageLengthInBytes() + indexStorage.getRowIdRlePageLengthInBytes()
            + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      }
      if (indexStorage.getDataRlePageLengthInBytes() > 0) {
        dataSize += indexStorage.getDataRlePageLengthInBytes();
      }
      dataSize += getEncodedData().length;
    } else {
      dataSize = getEncodedData().length;
    }
    return dataSize + metadataSize;
  }

  @Override
  public ByteBuffer serializeData() {
    ByteBuffer buffer = ByteBuffer.allocate(getSerializedDataSize());
    buffer.put(encodedData);
    if (indexStorage != null) {
      if (indexStorage.getRowIdPageLengthInBytes() > 0) {
        buffer.putInt(indexStorage.getRowIdPageLengthInBytes());
        short[] rowIdPage = (short[]) indexStorage.getRowIdPage();
        for (short rowId : rowIdPage) {
          buffer.putShort(rowId);
        }
        if (indexStorage.getRowIdRlePageLengthInBytes() > 0) {
          short[] rowIdRlePage = (short[]) indexStorage.getRowIdRlePage();
          for (short rowIdRle : rowIdRlePage) {
            buffer.putShort(rowIdRle);
          }
        }
      }
      if (indexStorage.getDataRlePageLengthInBytes() > 0) {
        short[] dataRlePage = (short[]) indexStorage.getDataRlePage();
        for (short dataRle : dataRlePage) {
          buffer.putShort(dataRle);
        }
      }
    }
    buffer.flip();
    return buffer;
  }

  private void fillDataChunk2NewWay(DataChunk2 dataChunk) throws IOException {
    Encoding encoding = encodings.get(0);
    ValueEncoderMeta metaData = metaDatas.get(0);
    assert (metaData instanceof ColumnPageEncoderMeta);
    ColumnPageEncoderMeta meta = (ColumnPageEncoderMeta) metaData;
    ByteBuffer buffer = serializeEncodingMetaData(encoding, meta);
    List<ByteBuffer> encoderMetaList = new ArrayList<ByteBuffer>();
    encoderMetaList.add(buffer);
    dataChunk.setEncoder_meta(encoderMetaList);

    if (dimensionType == DimensionType.PLAIN_VALUE) {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(meta.getMaxAsBytes())));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(meta.getMinAsBytes())));
    } else {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(meta.getMaxAsBytes()));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(meta.getMinAsBytes()));
    }
  }

  private void fillDataChunk2OldWay(DataChunk2 dataChunk) {
    if (indexStorage.getDataRlePageLengthInBytes() > 0 ||
        dimensionType == DimensionType.GLOBAL_DICTIONARY) {
      dataChunk.setRle_page_length(indexStorage.getDataRlePageLengthInBytes());
    }
    SortState sort = (indexStorage.getRowIdPageLengthInBytes() > 0) ?
        SortState.SORT_EXPLICIT : SortState.SORT_NATIVE;
    dataChunk.setSort_state(sort);
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      dataChunk.setRowid_page_length(getTotalRowIdPageLengthInBytes());
    }
    if (dimensionType == DimensionType.PLAIN_VALUE) {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(indexStorage.getMax())));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(indexStorage.getMin())));
    } else {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(indexStorage.getMax()));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(indexStorage.getMin()));
    }
  }

  public IndexStorage getIndexStorage() {
    return indexStorage;
  }

  public DimensionType getDimensionType() {
    return dimensionType;
  }
  */
}