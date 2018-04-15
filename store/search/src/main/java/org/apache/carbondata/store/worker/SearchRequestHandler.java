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

package org.apache.carbondata.store.worker;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport;
import org.apache.carbondata.store.protocol.SearchRequest;
import org.apache.carbondata.store.protocol.SearchResult;
import org.apache.carbondata.store.util.GrpcSerdes;

import com.google.protobuf.ByteString;

/**
 * Thread runnable for handling SearchRequest from master.
 */
@InterfaceAudience.Internal
class SearchRequestHandler implements Runnable {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SearchRequestHandler.class.getName());
  private boolean running = true;
  private Queue<SearchService.SearchRequestContext> requestQueue;

  SearchRequestHandler(Queue<SearchService.SearchRequestContext> requestQueue) {
    this.requestQueue = requestQueue;
  }

  public void run() {
    while (running) {
      SearchService.SearchRequestContext requestContext = requestQueue.poll();
      if (requestContext == null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      } else {
        try {
          List<CarbonRow> rows = handleRequest(requestContext);
          sendSuccessResponse(requestContext, rows);
        } catch (IOException | InterruptedException e) {
          LOG.error(e);
          sendFailureResponse(requestContext, e);
        }
      }
    }
  }

  public void stop() {
    running = false;
  }

  /**
   * Builds {@link QueryModel} and read data from files
   */
  private List<CarbonRow> handleRequest(SearchService.SearchRequestContext requestContext)
      throws IOException, InterruptedException {
    SearchRequest request = requestContext.getRequest();
    TableInfo tableInfo = GrpcSerdes.deserialize(request.getTableInfo());
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    QueryModel queryModel = createQueryModel(table, request);

    // the request contains CarbonMultiBlockSplit and reader will read multiple blocks
    // by using a thread pool
    CarbonMultiBlockSplit mbSplit = getMultiBlockSplit(request);

    // If there is FGDataMap, prune the split by applying FGDataMap
    queryModel = tryPruneByFGDataMap(table, queryModel, mbSplit);

    CarbonRecordReader<CarbonRow> reader =
        new CarbonRecordReader<>(queryModel, new CarbonRowReadSupport());
    reader.initialize(mbSplit, null);

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try {
      while (reader.nextKeyValue()) {
        rows.add(reader.getCurrentValue());
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return rows;
  }

  /**
   * If there is FGDataMap defined for this table and filter condition in the query,
   * prune the splits by the DataMap and set the pruned split into the QueryModel and return
   */
  private QueryModel tryPruneByFGDataMap(
      CarbonTable table, QueryModel queryModel, CarbonMultiBlockSplit mbSplit) throws IOException {
    DataMapExprWrapper wrapper =
        DataMapChooser.get().choose(table, queryModel.getFilterExpressionResolverTree());

    if (wrapper.getDataMapType() == DataMapLevel.FG) {
      List<Segment> segments = new LinkedList<>();
      for (CarbonInputSplit split : mbSplit.getAllSplits()) {
        segments.add(Segment.toSegment(split.getSegmentId()));
      }
      List<ExtendedBlocklet> prunnedBlocklets =
          wrapper.prune(segments, null, new LatestFilesReadCommittedScope(table.getTablePath()));

      List<String> pathToRead = new LinkedList<>();
      for (ExtendedBlocklet prunnedBlocklet : prunnedBlocklets) {
        pathToRead.add(prunnedBlocklet.getPath());
      }

      List<TableBlockInfo> blocks = queryModel.getTableBlockInfos();
      List<TableBlockInfo> blockToRead = new LinkedList<>();
      for (TableBlockInfo block : blocks) {
        if (pathToRead.contains(block.getFilePath())) {
          blockToRead.add(block);
        }
      }
      queryModel.setTableBlockInfos(blockToRead);
    }
    return queryModel;
  }

  private CarbonMultiBlockSplit getMultiBlockSplit(SearchRequest request) throws IOException {
    ByteString splits = request.getSplits();
    CarbonMultiBlockSplit mbSplit = new CarbonMultiBlockSplit();
    ByteArrayInputStream stream = new ByteArrayInputStream(splits.toByteArray());
    DataInputStream inputStream = new DataInputStream(stream);
    mbSplit.readFields(inputStream);
    return mbSplit;
  }

  private QueryModel createQueryModel(CarbonTable table, SearchRequest request) throws IOException {
    String[] projectColumns = new String[request.getProjectColumnsCount()];
    for (int i = 0; i < request.getProjectColumnsCount(); i++) {
      projectColumns[i] = request.getProjectColumns(i);
    }
    Expression filter;
    if (request.getFilterExpression().isEmpty()) {
      filter = null;
    } else {
      filter = GrpcSerdes.deserialize(request.getFilterExpression());
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }

  /**
   * Send failure response to master
   */
  private void sendFailureResponse(
      SearchService.SearchRequestContext requestContext,
      Throwable throwable) {
    SearchResult response = SearchResult.newBuilder()
        .setQueryId(requestContext.getRequest().getQueryId())
        .setStatus(SearchResult.Status.FAILURE)
        .setMessage(throwable.getMessage())
        .build();
    requestContext.getResponseObserver().onNext(response);
    requestContext.getResponseObserver().onCompleted();
  }

  /**
   * send success response with result rows to master
   */
  private void sendSuccessResponse(
      SearchService.SearchRequestContext requestContext,
      List<CarbonRow> rows) throws IOException {
    SearchResult.Builder builder = SearchResult.newBuilder()
        .setQueryId(requestContext.getRequest().getQueryId())
        .setStatus(SearchResult.Status.SUCCESS)
        .setMessage("SUCCESS");
    for (CarbonRow row : rows) {
      builder.addRow(GrpcSerdes.serialize(row));
    }
    SearchResult response = builder.build();
    requestContext.getResponseObserver().onNext(response);
    requestContext.getResponseObserver().onCompleted();
  }
}
