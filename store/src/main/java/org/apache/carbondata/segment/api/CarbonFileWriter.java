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

package org.apache.carbondata.segment.api;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.processing.loading.iterator.CarbonOutputIteratorWrapper;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.io.NullWritable;

public class CarbonFileWriter {

  private CarbonTableOutputFormat.CarbonRecordWriter recordWriter;
  private StringArrayWritable writable = new StringArrayWritable();

  private CarbonFileWriter(final CarbonLoadModel loadModel) {
    loadModel.setTaskNo(System.nanoTime() + "");
    final String[] tempStoreLocations = new String[] {
        System.getProperty("java.io.tmpdir") + "/" + System.nanoTime()};
    final CarbonOutputIteratorWrapper iteratorWrapper = new CarbonOutputIteratorWrapper();
    final DataLoadExecutor dataLoadExecutor = new DataLoadExecutor();
    ExecutorService executorService = Executors.newFixedThreadPool(
        1, new CarbonThreadFactory("CarbonRecordWriter:" + loadModel.getTableName()));
    // It should be started in new thread as the underlying iterator uses blocking queue.
    Future future = executorService.submit(new Thread() {
      @Override public void run() {
        try {
          dataLoadExecutor.execute(
              loadModel, tempStoreLocations, new CarbonIterator[] { iteratorWrapper });
        } catch (Exception e) {
          dataLoadExecutor.close();
          throw new RuntimeException(e);
        }
      }
    });
    this.recordWriter =
        new CarbonTableOutputFormat.CarbonRecordWriter(iteratorWrapper, dataLoadExecutor,
            loadModel, future, executorService);
  }

  public static CarbonFileWriter newInstance(CarbonLoadModel loadModel) {
    return new CarbonFileWriter(loadModel);
  }

  public void writeRow(String[] fields) throws IOException {
    writable.set(fields);
    try {
      recordWriter.write(NullWritable.get(), writable);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void close() throws IOException {
    try {
      recordWriter.close(null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
