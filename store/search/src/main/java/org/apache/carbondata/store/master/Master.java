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

package org.apache.carbondata.store.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.store.protocol.EchoRequest;
import org.apache.carbondata.store.protocol.EchoResponse;
import org.apache.carbondata.store.protocol.SearchRequest;
import org.apache.carbondata.store.protocol.SearchResult;
import org.apache.carbondata.store.protocol.ShutdownRequest;
import org.apache.carbondata.store.protocol.ShutdownResponse;
import org.apache.carbondata.store.protocol.WorkerGrpc;
import org.apache.carbondata.store.util.GrpcSerdes;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

/**
 * Master of CarbonSearch.
 * It listens to {@link Master#DEFAULT_PORT} to wait for worker to register.
 * And it provides search API to fire RPC call to workers.
 */
@InterfaceAudience.Internal
public class Master {

  private static final LogService LOG = LogServiceFactory.getLogService(Master.class.getName());

  public static final int DEFAULT_PORT = 10020;

  private Server registryServer;

  private int port;

  private Random random = new Random();

  /** mapping of worker IP to rpc stub */
  private Map<String, WorkerGrpc.WorkerFutureStub> workers;

  public Master() {
    this(DEFAULT_PORT);
  }

  public Master(int port) {
    this.port = port;
    this.workers = new ConcurrentHashMap<>();
  }

  /** start service and listen on port passed in constructor */
  public void startService() throws IOException {
    if (registryServer == null) {
      /* The port on which the registryServer should run */
      registryServer = ServerBuilder.forPort(port)
          .addService(new RegistryService(this))
          .build()
          .start();
      LOG.info("Master started, listening on " + port);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override public void run() {
          // Use stderr here since the logger may have been reset by its JVM shutdown hook.
          LOG.info("*** shutting down gRPC Master since JVM is shutting down");
          stopService();
          LOG.info("*** Master shut down");
        }
      });
    }
  }

  public void stopService() {
    if (registryServer != null) {
      registryServer.shutdown();
    }
  }

  public void stopAllWorkers() throws IOException, ExecutionException, InterruptedException {
    ShutdownRequest request = ShutdownRequest.newBuilder()
        .setTrigger(ShutdownRequest.Trigger.USER)
        .build();
    for (Map.Entry<String, WorkerGrpc.WorkerFutureStub> worker : workers.entrySet()) {
      ListenableFuture<ShutdownResponse> future = worker.getValue().shutdown(request);
      ShutdownResponse response = future.get();
      if (response.getStatus() != ShutdownResponse.Status.SUCCESS) {
        LOG.error("failed to shutdown worker: " + response.getMessage());
        throw new IOException(response.getMessage());
      } else {
        workers.remove(worker.getKey());
      }
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (registryServer != null) {
      registryServer.awaitTermination();
    }
  }

  /** A new searcher is trying to register, add it to the map and connect to this searcher */
  void addWorker(String workerId, String workerHostname, int port, int cores)
      throws ExecutionException, InterruptedException {
    Objects.requireNonNull(workerHostname);

    LOG.info(String.format("connecting to worker %s [%s:%d]", workerId, workerHostname, port));
    ManagedChannel channelToWorker = ManagedChannelBuilder.forAddress(workerHostname, port)
        .usePlaintext(true)
        .maxInboundMessageSize(200 * 1000 * 1000)
        .build();
    WorkerGrpc.WorkerFutureStub futureStub = WorkerGrpc.newFutureStub(channelToWorker);

    // try to send a message to worker as a test
    tryEcho(futureStub);
    workers.put(workerHostname, futureStub);
    LOG.info(String.format("worker %s [%s:%d] added", workerId, workerHostname, port));
  }

  private void tryEcho(WorkerGrpc.WorkerFutureStub stub)
      throws ExecutionException, InterruptedException {
    EchoRequest request = EchoRequest.newBuilder().setMessage("hello").build();
    LOG.info("echo to searcher: " + request.getMessage());
    ListenableFuture<EchoResponse> response = stub.echo(request);
    try {
      LOG.info("echo from searcher: " + response.get().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("failed to echo: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Execute search by firing RPC call to worker, return the result rows
   */
  public CarbonRow[] search(CarbonTable table, String[] columns, Expression filter)
      throws IOException, InvalidConfigurationException, ExecutionException, InterruptedException {
    Objects.requireNonNull(table);
    Objects.requireNonNull(columns);

    if (workers.size() == 0) {
      throw new IOException("No worker is available");
    }

    int queryId = random.nextInt();

    // Build a SearchRequest
    SearchRequest.Builder builder = SearchRequest.newBuilder()
        .setQueryId(queryId)
        .setTableInfo(GrpcSerdes.serialize(table.getTableInfo()));
    for (String column : columns) {
      builder.addProjectColumns(column);
    }
    if (filter != null) {
      builder.setFilterExpression(GrpcSerdes.serialize(filter));
    }

    // prune data and get a mapping of worker hostname to list of blocks,
    // add these blocks to the SearchRequest and fire the RPC call
    Map<String, List<Distributable>> nodeBlockMapping = pruneBlock(table, columns, filter);

    List<ListenableFuture<SearchResult>> futures = new ArrayList<>(nodeBlockMapping.size());

    for (Map.Entry<String, List<Distributable>> entry : nodeBlockMapping.entrySet()) {
      String workerIP = entry.getKey();
      List<Distributable> blocks = entry.getValue();
      CarbonMultiBlockSplit mbSplit = new CarbonMultiBlockSplit(blocks, workerIP);
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutput dataOutput = new DataOutputStream(stream);
      mbSplit.write(dataOutput);
      builder.setSplits(ByteString.copyFrom(stream.toByteArray()));

      SearchRequest request = builder.build();

      // do RPC to worker asynchronously and concurrently
      ListenableFuture<SearchResult> future = workers.get(workerIP).search(request);
      futures.add(future);
    }

    // get all results from RPC response and return to caller
    List<CarbonRow> output = new LinkedList<>();
    for (ListenableFuture<SearchResult> future : futures) {
      SearchResult result = future.get();
      if (result.getQueryId() != queryId) {
        throw new IOException(String.format(
            "queryId in response does not match request: %d != %d", result.getQueryId(), queryId));
      }
      collectResult(result, output);
    }
    return output.toArray(new CarbonRow[output.size()]);
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of hostname to list of block
   */
  private Map<String, List<Distributable>> pruneBlock(CarbonTable table, String[] columns,
      Expression filter) throws IOException, InvalidConfigurationException {
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat<Object> format = CarbonInputFormatUtil.createCarbonTableInputFormat(
        job, table, columns, filter, null, null);

    List<InputSplit> splits = format.getSplits(job);
    List<Distributable> distributables = new ArrayList<>(splits.size());
    for (InputSplit split : splits) {
      distributables.add(((CarbonInputSplit)split));
    }
    return CarbonLoaderUtil.nodeBlockMapping(
        distributables, -1, new ArrayList<String>(workers.keySet()),
        CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST);
  }

  /**
   * Fill result row to {@param output}
   */
  private void collectResult(SearchResult result,  List<CarbonRow> output) throws IOException {
    for (ByteString bytes : result.getRowList()) {
      CarbonRow row = GrpcSerdes.deserialize(bytes);
      output.add(row);
    }
  }

  /** return IP of all workers */
  public Set<String> getWorkers() {
    return workers.keySet();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Master master = new Master(DEFAULT_PORT);
    master.startService();
    master.blockUntilShutdown();
  }
}
