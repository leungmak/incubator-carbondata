/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow.steps;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class CarbonRowWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataWriterProcessorStepImpl.class.getName());

  private SegmentProperties segmentProperties;

  private KeyGenerator keyGenerator;

  private CarbonFactHandler dataHandler;

  private int noDictionaryCount;

  private int complexDimensionCount;

  private int measureCount;

  private long readCounter;

  private long writeCounter;

  private int measureIndex = IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex();

  private int noDimByteArrayIndex = IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex();

  private int dimsArrayIndex = IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex();

  private String storeLocation;

  public CarbonRowWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override
  public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void initialize() throws CarbonDataLoadingException {
    child.initialize();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();

    storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), String.valueOf(configuration.getTaskNo()),
            configuration.getPartitionId(), configuration.getSegmentId() + "", false);

    if (!(new File(storeLocation).mkdirs())) {
      LOGGER.error("Local data load folder location does not exist: " + storeLocation);
      return;
    }
  }

  @Override
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    String tableName = configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName();
    try {
      CarbonFactDataHandlerModel dataHandlerModel =
          CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(configuration, storeLocation);
      noDictionaryCount = dataHandlerModel.getNoDictionaryCount();
      complexDimensionCount = configuration.getComplexDimensionCount();
      measureCount = dataHandlerModel.getMeasureCount();
      segmentProperties = dataHandlerModel.getSegmentProperties();
      keyGenerator = segmentProperties.getDimensionKeyGenerator();
      dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(dataHandlerModel,
          CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
      dataHandler.initialise();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
              System.currentTimeMillis());
      for (Iterator<CarbonRowBatch> iterator : iterators) {
        while (iterator.hasNext()) {
          processBatch(iterator.next());
        }
      }

    } catch (CarbonDataWriterException e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
    }
    return null;
  }

  @Override
  public void close() {
    String tableName = configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName();
    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    LOGGER.info("Record Processed For table: " + tableName);
    String logMessage =
        "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter + ": Write: "
            + writeCounter;
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(writeCounter);
    processingComplete();
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(configuration.getPartitionId(), System.currentTimeMillis());
  }

  private void processingComplete() {
    if (null != dataHandler) {
      dataHandler.closeHandler();
    }
  }

  private void processBatch(CarbonRowBatch batch) throws CarbonDataLoadingException {
    Iterator<CarbonRow> iterator = batch.getBatchIterator();
    boolean[] noDictionaryMapping = CarbonDataProcessorUtil.getNoDictionaryMapping(getOutput());
    int complexOffset = getOutput().length - measureCount - complexDimensionCount;

    while (iterator.hasNext()) {
      CarbonRow row = iterator.next();
      readCounter++;
      Object[] outputRow;
      // adding one for the high cardinality dims byte array.
      if (noDictionaryCount > 0 || complexDimensionCount > 0) {
        outputRow = new Object[measureCount + 1 + 1];
      } else {
        outputRow = new Object[measureCount + 1];
      }

      int l = 0;
      int index = 0;

      // convert row to 3-elements row
      // the input row is dimension(N), measure(N)
      // the outputRow should be measure(N object), noDictAndComplexColumn(1 byte[][]), dict(1 byte[], mdk)
      // fill measure(N object)
      int dimensionCount = row.size() - measureCount;
      int columnIndex = 0;
      for (; columnIndex < measureCount; columnIndex++) {
        outputRow[columnIndex] = row.getObject(columnIndex + dimensionCount);
      }
      // no dictionary + complex column data
      byte[][] noDictAndComplexData = new byte[noDictionaryCount + complexDimensionCount][];
      // dictionary data
      int[] dictData = new int[dimensionCount - noDictionaryCount];
      outputRow[columnIndex++] = noDictAndComplexData;
      int noDictcount = 0;
      int dictCount = 0;
      for (int i = 0; i < noDictionaryMapping.length; i++){
        if (noDictionaryMapping[i]) {
          noDictAndComplexData[noDictcount++] = row.getBinary(i);
        } else {
          dictData[dictCount++] = row.getInt(i);
        }
      }
      for (int i =0; i < complexDimensionCount; i++) {
        noDictAndComplexData[noDictcount++] = row.getBinary(complexOffset + i);
      }
      // generate MDK
      try {
        outputRow[outputRow.length - 1] = keyGenerator.generateKey(dictData);
      } catch (Exception e) {
        throw new CarbonDataLoadingException("unable to generate the mdkey", e);
      }
      dataHandler.addDataToStore(outputRow);
      writeCounter++;
    }
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

}
