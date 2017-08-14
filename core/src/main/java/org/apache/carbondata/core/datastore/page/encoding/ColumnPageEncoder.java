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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datastore.DimensionType;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.TablePageStatistics;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.PresenceMeta;

public abstract class ColumnPageEncoder {

  protected abstract byte[] encodeData(ColumnPage input) throws MemoryException, IOException;

  protected abstract List<Encoding> getEncodingList();

  protected abstract ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage);

  protected abstract void fillLegacyFields(ColumnPage inputPage, DataChunk2 dataChunk) throws IOException;

  /**
   * Return a encoded column page by encoding the input page
   * The encoded binary data and metadata are wrapped in encoding column page
   */
  public EncodedColumnPage encode(ColumnPage inputPage) throws IOException, MemoryException {
    byte[] encodedBytes = encodeData(inputPage);
    DataChunk2 pageHeader = buildPageHeader(inputPage, encodedBytes);
    return new EncodedColumnPage(pageHeader, encodedBytes, inputPage.getNullBits(), inputPage.getStatistics());
  }

  private DataChunk2 buildPageHeader(ColumnPage inputPage, byte[] encodedBytes) throws IOException {
    DataChunk2 dataChunk = new DataChunk2();
    fillBasicFields(inputPage, dataChunk);
    fillNullBitSet(inputPage, dataChunk);
    fillEncoding(inputPage, dataChunk);
    fillMinMaxIndex(inputPage, dataChunk);
    fillLegacyFields(inputPage, dataChunk);
    dataChunk.setData_page_length(encodedBytes.length);
    return dataChunk;
  }

  private void fillBasicFields(ColumnPage inputPage, DataChunk2 dataChunk) {
    dataChunk.setChunk_meta(CarbonMetadataUtil.getSnappyChunkCompressionMeta());
    dataChunk.setNumberOfRowsInpage(inputPage.getPageSize());
    dataChunk.setRowMajor(false);
  }

  private void fillNullBitSet(ColumnPage inputPage, DataChunk2 dataChunk) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setPresent_bit_streamIsSet(true);
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    presenceMeta.setPresent_bit_stream(compressor.compressByte(inputPage.getNullBits().toByteArray()));
    dataChunk.setPresence(presenceMeta);
  }

  private void fillEncoding(ColumnPage inputPage, DataChunk2 dataChunk) throws IOException {
    dataChunk.setEncoders(getEncodingList());
    dataChunk.setEncoder_meta(buildEncoderMeta(inputPage));
  }

  private List<ByteBuffer> buildEncoderMeta(ColumnPage inputPage) throws IOException {
    ColumnPageEncoderMeta meta = getEncoderMeta(inputPage);
    List<ByteBuffer> metaDatas = new ArrayList<>();
    if (meta != null) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(stream);
      meta.write(out);
      metaDatas.add(ByteBuffer.wrap(stream.toByteArray()));
    }
    return metaDatas;
  }

  private void fillMinMaxIndex(ColumnPage inputPage, DataChunk2 dataChunk) {
    dataChunk.setMin_max(buildMinMaxIndex(inputPage));
  }

  private BlockletMinMaxIndex buildMinMaxIndex(ColumnPage inputPage) {
    // for measure page
    BlockletMinMaxIndex index = new BlockletMinMaxIndex();
    ByteBuffer max = ByteBuffer.wrap(
        TablePageStatistics.updateMinMaxForNoDictionary(CarbonUtil.getValueAsBytes(inputPage.getDataType(), inputPage.getStatistics().getMax())));
    ByteBuffer min = ByteBuffer.wrap(
        TablePageStatistics.updateMinMaxForNoDictionary(CarbonUtil.getValueAsBytes(inputPage.getDataType(), inputPage.getStatistics().getMin())));
    index.addToMax_values(max);
    index.addToMin_values(min);
    return index;
    // TODO: case for dimension page
  }

  public IndexStorage getIndexStorage() {
    return null;
  }

  /**
   * Apply encoding algorithm for complex column page and return the coded data
   * TODO: remove this interface after complex column page is unified with column page
   */

  public EncodedColumnPage[] encodeComplexColumn(ComplexColumnPage input) {
    EncodedColumnPage[] encodedPages = new EncodedColumnPage[input.getDepth()];
    int index = 0;
    Iterator<byte[][]> iterator = input.iterator();
    while (iterator.hasNext()) {
      byte[][] data = iterator.next();
      encodedPages[index++] = encodeChildColumn(input.getPageSize(), data);
    }
    return encodedPages;
  }

  private EncodedColumnPage encodeChildColumn(int pageSize, byte[][] data) {
//    IndexStorage indexStorage;
//    ColumnarFormatVersion version = CarbonProperties.getInstance().getFormatVersion();
//    if (version == ColumnarFormatVersion.V3) {
//      indexStorage = new BlockIndexerStorageForShort(data, false, false, false);
//    } else {
//      indexStorage = new BlockIndexerStorageForInt(data, false, false, false);
//    }
//    byte[] flattened = ByteUtil.flatten(indexStorage.getDataPage());
//    Compressor compressor = CompressorFactory.getInstance().getCompressor();
//    byte[] compressed = compressor.compressByte(flattened);
//    List<Encoding> encodings = new ArrayList<>();
//    encodings.add(Encoding.DICTIONARY);
//    return new EncodedDimensionPage(
//        pageSize,
//        compressed,
//        null, // TODO: need to support null bitset for complex column
//        encodings,
//        null,
//        DimensionType.COMPLEX,
//        indexStorage);
    return null;
  }

}
