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

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.scan.executor.impl.SearchModeVectorDetailQueryExecutor;
import org.apache.carbondata.store.master.Master;
import org.apache.carbondata.store.protocol.MasterGrpc;
import org.apache.carbondata.store.protocol.RegisterWorkerRequest;
import org.apache.carbondata.store.protocol.RegisterWorkerResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Worker of CarbonSearch.
 */
@InterfaceAudience.Internal
public class Worker {

  private static final LogService LOG = LogServiceFactory.getLogService(Worker.class.getName());

  private static final int DEFAULT_PORT = 10021;

  private ManagedChannel channelToMaster;

  private Server server;

  private boolean registered = false;

  private int port;

  private static Worker INSTANCE;

  public static synchronized Worker getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new Worker();
    }
    return INSTANCE;
  }

  private Worker() {
    this(DEFAULT_PORT);
  }

  private Worker(int port) {
    this.port = port;
  }

  /**
   * start worker service and register to master
   */
  public synchronized void init(String masterHostname, int masterPort) throws IOException {
    LOG.info("starting worker service...");
    startService();
    LOG.info("registering to master...");
    registerToMaster(masterHostname, masterPort);
    LOG.info("worker initialization finished");
  }

  /** It will start listen on the port for search service */
  private void startService() throws IOException {
    if (server == null) {
      /* The port on which the SearchService should run */
      server = ServerBuilder.forPort(port)
          .addService(new SearchService(this, 10))
          .build()
          .start();
      LOG.info("Worker started, listening on " + port);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          // Use stderr here since the LOG may have been reset by its JVM shutdown hook.
          LOG.info("Shutting down gRPC Worker since JVM is shutting down");
          stopService();
          LOG.info("Worker shut down");
        }
      });
    }
  }

  private void stopService() {
    if (server != null) {
      LOG.info("Shutting down worker");
      server.shutdown();
      server = null;
      registered = false;
    }
  }

  /** Send RegisterWorker message to Master. */
  private void registerToMaster(String masterHostname, int masterPort) throws IOException {
    if (registered) {
      return;
    }

    LOG.info("Registering to driver " + masterHostname + ":" + masterPort);
    // Construct client connecting to Master server at {@code host:port}.
    this.channelToMaster = ManagedChannelBuilder.forAddress(masterHostname, masterPort)
        .usePlaintext(true)
        .build();
    MasterGrpc.MasterBlockingStub blockingStub = MasterGrpc.newBlockingStub(channelToMaster);
    int cores = Runtime.getRuntime().availableProcessors();
    String searcherHostname = InetAddress.getLocalHost().getHostName();
    RegisterWorkerRequest request =
        RegisterWorkerRequest.newBuilder()
            .setHostname(searcherHostname)
            .setPort(port)
            .setCores(cores)
            .build();
    RegisterWorkerResponse response;
    try {
      response = blockingStub.registerWorker(request);
    } catch (StatusRuntimeException e) {
      LOG.error(e, "RPC failed: " + e.getStatus());
      return;
    }
    registered = true;
    LOG.info("Register response from master: " + response.getMessage());
  }

  /** shutdown the channel and all threads in reader */
  public void shutdown() throws InterruptedException {
    if (channelToMaster != null) {
      channelToMaster.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    stopService();
    SearchModeVectorDetailQueryExecutor.shutdownThreadPool();
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    String hostname = InetAddress.getLocalHost().getHostAddress();
    Worker worker = Worker.getInstance();
    worker.registerToMaster(hostname, Master.DEFAULT_PORT);
    worker.startService();
    worker.blockUntilShutdown();
  }

}
