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
package org.apache.carbondata.processing.store.writer.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.encoding.EncodedDimensionPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;

/**
 * Below class will be used to write the data in V3 format
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 */
public class CarbonFactDataWriterImplV3 extends AbstractFactDataWriter<short[]> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV3.class.getName());

  /**
   * persist the page data to be written in the file
   */
  private DataWriterHolder dataWriterHolder;

  private long blockletSize;

  public CarbonFactDataWriterImplV3(CarbonDataWriterVo dataWriterVo) {
    super(dataWriterVo);
    blockletSize = Long.parseLong(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE))
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    if (blockletSize > fileSizeInBytes) {
      blockletSize = fileSizeInBytes;
      LOGGER.info("Blocklet size configure for table is: " + blockletSize);
    }
    dataWriterHolder = new DataWriterHolder();
  }

  @Override protected void writeBlockletInfoToFile(FileChannel channel, String filePath)
      throws CarbonDataWriterException {
    try {
      // get the current file position
      long currentPosition = channel.size();
      // get thrift file footer instance
      FileFooter3 convertFileMeta = CarbonMetadataUtil
          .convertFileFooterVersion3(blockletMetadata, blockletIndex, localCardinality,
              thriftColumnSchemaList.size());
      // fill the carbon index details
      fillBlockIndexInfoDetails(convertFileMeta.getNum_rows(), carbonDataFileName, currentPosition);
      // write the footer
      byte[] byteArray = CarbonUtil.getByteArray(convertFileMeta);
      ByteBuffer buffer =
          ByteBuffer.allocate(byteArray.length + CarbonCommonConstants.LONG_SIZE_IN_BYTE);
      buffer.put(byteArray);
      buffer.putLong(currentPosition);
      buffer.flip();
      channel.write(buffer);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }

  /**
   * Below method will be used to write one table page data
   */
  @Override public void writeTablePage(EncodedTablePage encoded)
      throws CarbonDataWriterException {
    // condition for writting all the pages
    if (!encoded.isLastPage()) {
      boolean isAdded = false;
      // check if size more than blocklet size then write the page to file
      if (dataWriterHolder.getSize() + encoded.getEncodedSize() >= blockletSize) {
        // if one page size is more than blocklet size
        if (dataWriterHolder.getEncodedTablePages().size() == 0) {
          isAdded = true;
          dataWriterHolder.addPage(encoded);
        }

        LOGGER.info("Number of Pages for blocklet is: " + dataWriterHolder.getNumberOfPagesAdded()
            + " :Rows Added: " + dataWriterHolder.getTotalRows());
        // write the data
        writeDataToFile(fileChannel);
      }
      if (!isAdded) {
        dataWriterHolder.addPage(encoded);
      }
    } else {
      //for last blocklet check if the last page will exceed the blocklet size then write
      // existing pages and then last page
      if (encoded.getPageSize() > 0) {
        dataWriterHolder.addPage(encoded);
      }
      if (dataWriterHolder.getNumberOfPagesAdded() > 0) {
        LOGGER.info("Number of Pages for blocklet is: " + dataWriterHolder.getNumberOfPagesAdded()
            + " :Rows Added: " + dataWriterHolder.getTotalRows());
        writeDataToFile(fileChannel);
      }
    }
  }

  private void writeDataToFile(FileChannel channel) {
    // get the list of node holder list
    List<EncodedTablePage> encodedTablePageList = dataWriterHolder.getEncodedTablePages();
    int numDimensions = encodedTablePageList.get(0).getNumDimensions();
    int numMeasures = encodedTablePageList.get(0).getNumMeasures();
    long blockletDataSize = 0;
    // get data chunks for all the column
    byte[][] dataChunkBytes = new byte[numDimensions + numMeasures][];
    int measureStartIndex = numDimensions;
    // calculate the size of data chunks
    try {
      for (int i = 0; i < numDimensions; i++) {
        dataChunkBytes[i] = CarbonUtil.getByteArray(
            EncodedDimensionPage.getDataChunk3(encodedTablePageList, i));
        blockletDataSize += dataChunkBytes[i].length;
      }
      for (int i = 0; i < numMeasures; i++) {
        dataChunkBytes[measureStartIndex] = CarbonUtil.getByteArray(
            EncodedMeasurePage.getDataChunk3(encodedTablePageList, i));
        blockletDataSize += dataChunkBytes[measureStartIndex].length;
        measureStartIndex++;
      }
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the data chunks", e);
    }
    // calculate the total size of data to be written
    blockletDataSize += dataWriterHolder.getSize();
    // to check if data size will exceed the block size then create a new file
    updateBlockletFileChannel(blockletDataSize);
    // write data to file
    writeDataToFile(channel, dataChunkBytes);
    // clear the data holder
    dataWriterHolder.clear();
  }

  /**
   * Below method will be used to write data in carbon data file
   * Data Format
   * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
   * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
   * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
   * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
   * Each page will contain column data, Inverted index and rle index
   *
   * @param channel
   * @param dataChunkBytes
   */
  private void writeDataToFile(FileChannel channel, byte[][] dataChunkBytes) {
    long offset = 0;
    // write the header
    try {
      if (channel.size() == 0) {
        // below code is to write the file header
        byte[] fileHeader = CarbonUtil.getByteArray(CarbonMetadataUtil
            .getFileHeader(true, thriftColumnSchemaList, dataWriterVo.getSchemaUpdatedTimeStamp()));
        ByteBuffer buffer = ByteBuffer.wrap(fileHeader);
        channel.write(buffer);
      }
      offset = channel.size();
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while getting the file channel size");
    }
    // to maintain the offset of each data chunk in blocklet
    List<Long> currentDataChunksOffset = new ArrayList<>();
    // to maintain the length of each data chunk in blocklet
    List<Integer> currentDataChunksLength = new ArrayList<>();
    // get the node holder list
    List<EncodedTablePage> encodedTablePages = dataWriterHolder.getEncodedTablePages();
    int numberOfDimension = encodedTablePages.get(0).getNumDimensions();
    int numberOfMeasures = encodedTablePages.get(0).getNumMeasures();
    ByteBuffer buffer = null;
    long dimensionOffset = 0;
    long measureOffset = 0;
    int numberOfRows = 0;
    // calculate the number of rows in each blocklet
    for (EncodedTablePage encodedTablePage1 : encodedTablePages) {
      numberOfRows += encodedTablePage1.getPageSize();
    }
    try {
      for (int i = 0; i < numberOfDimension; i++) {
        currentDataChunksOffset.add(offset);
        currentDataChunksLength.add(dataChunkBytes[i].length);
        buffer = ByteBuffer.wrap(dataChunkBytes[i]);
        channel.write(buffer);
        offset += dataChunkBytes[i].length;
        for (EncodedTablePage encodedTablePage : encodedTablePages) {
          EncodedDimensionPage dimension = encodedTablePage.getDimension(i);
          int bufferSize = dimension.getSerializedSize();
          buffer = dimension.serialize();
          channel.write(buffer);
          offset += bufferSize;
        }
      }
      dimensionOffset = offset;
      int dataChunkStartIndex = encodedTablePages.get(0).getNumDimensions();
      for (int i = 0; i < numberOfMeasures; i++) {
        encodedTablePages = dataWriterHolder.getEncodedTablePages();
        currentDataChunksOffset.add(offset);
        currentDataChunksLength.add(dataChunkBytes[dataChunkStartIndex].length);
        buffer = ByteBuffer.wrap(dataChunkBytes[dataChunkStartIndex]);
        channel.write(buffer);
        offset += dataChunkBytes[dataChunkStartIndex].length;
        dataChunkStartIndex++;
        for (EncodedTablePage encodedTablePage : encodedTablePages) {
          EncodedMeasurePage measure = encodedTablePage.getMeasure(i);
          int bufferSize = measure.getSerializedSize();
          buffer = measure.serialize();
          channel.write(buffer);
          offset += bufferSize;
        }
      }
      measureOffset = offset;
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the data", e);
    }
    blockletIndex.add(
        CarbonMetadataUtil.getBlockletIndex(
            encodedTablePages, dataWriterVo.getSegmentProperties().getMeasures()));
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3(numberOfRows, currentDataChunksOffset, currentDataChunksLength,
            dimensionOffset, measureOffset, dataWriterHolder.getEncodedTablePages().size());
    blockletMetadata.add(blockletInfo3);
  }

  /**
   * Below method will be used to fill the block info details
   *
   * @param numberOfRows    number of rows in file
   * @param carbonDataFileName The name of carbonData file
   * @param currentPosition current offset
   */
  protected void fillBlockIndexInfoDetails(long numberOfRows, String carbonDataFileName,
      long currentPosition) {
    byte[][] currentMinValue = new byte[blockletIndex.get(0).min_max_index.max_values.size()][];
    byte[][] currentMaxValue = new byte[blockletIndex.get(0).min_max_index.max_values.size()][];
    for (int i = 0; i < currentMaxValue.length; i++) {
      currentMinValue[i] = blockletIndex.get(0).min_max_index.getMin_values().get(i).array();
      currentMaxValue[i] = blockletIndex.get(0).min_max_index.getMax_values().get(i).array();
    }
    byte[] minValue = null;
    byte[] maxValue = null;
    int measureStartIndex = currentMinValue.length - dataWriterVo.getMeasureCount();
    for (int i = 1; i < blockletIndex.size(); i++) {
      for (int j = 0; j < measureStartIndex; j++) {
        minValue = blockletIndex.get(i).min_max_index.getMin_values().get(j).array();
        maxValue = blockletIndex.get(i).min_max_index.getMax_values().get(j).array();
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[j], minValue) > 0) {
          currentMinValue[j] = minValue.clone();
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[j], maxValue) < 0) {
          currentMaxValue[j] = maxValue.clone();
        }
      }
      int measureIndex = 0;
      for (int j = measureStartIndex; j < currentMinValue.length; j++) {
        minValue = blockletIndex.get(i).min_max_index.getMin_values().get(j).array();
        maxValue = blockletIndex.get(i).min_max_index.getMax_values().get(j).array();

        if (CarbonMetadataUtil.compareMeasureData(currentMinValue[j], minValue,
            dataWriterVo.getSegmentProperties().getMeasures().get(measureIndex).getDataType())
            > 0) {
          currentMinValue[j] = minValue.clone();
        }
        if (CarbonMetadataUtil.compareMeasureData(currentMaxValue[j], maxValue,
            dataWriterVo.getSegmentProperties().getMeasures().get(measureIndex).getDataType())
            < 0) {
          currentMaxValue[j] = maxValue.clone();
        }
        measureIndex++;
      }
    }
    BlockletBTreeIndex btree =
        new BlockletBTreeIndex(blockletIndex.get(0).b_tree_index.getStart_key(),
            blockletIndex.get(blockletIndex.size() - 1).b_tree_index.getEnd_key());
    BlockletMinMaxIndex minmax = new BlockletMinMaxIndex();
    minmax.setMinValues(currentMinValue);
    minmax.setMaxValues(currentMaxValue);
    org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex blockletIndex =
        new org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex(btree, minmax);
    BlockIndexInfo blockIndexInfo =
        new BlockIndexInfo(numberOfRows, carbonDataFileName, currentPosition, blockletIndex);
    blockIndexInfoList.add(blockIndexInfo);
  }

  /**
   * Method will be used to close the open file channel
   *
   * @throws CarbonDataWriterException
   */
  public void closeWriter() throws CarbonDataWriterException {
    CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
    renameCarbonDataFile();
    copyCarbonDataFileToCarbonStorePath(
        this.carbonDataFileTempPath.substring(0, this.carbonDataFileTempPath.lastIndexOf('.')));
    try {
      writeIndexFile();
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the index file", e);
    }
    closeExecutorService();
  }

  @Override public void writeBlockletInfoToFile() throws CarbonDataWriterException {
    if (this.blockletMetadata.size() > 0) {
      writeBlockletInfoToFile(fileChannel, carbonDataFileTempPath);
    }
  }
}
