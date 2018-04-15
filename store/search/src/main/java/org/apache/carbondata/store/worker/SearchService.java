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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.store.protocol.EchoRequest;
import org.apache.carbondata.store.protocol.EchoResponse;
import org.apache.carbondata.store.protocol.SearchRequest;
import org.apache.carbondata.store.protocol.SearchResult;
import org.apache.carbondata.store.protocol.ShutdownRequest;
import org.apache.carbondata.store.protocol.ShutdownResponse;
import org.apache.carbondata.store.protocol.WorkerGrpc;

import io.grpc.stub.StreamObserver;

/**
 * Search service implementation.
 * It queues the request from master, and handle it by a thread pool
 */
@InterfaceAudience.Internal
class SearchService extends WorkerGrpc.WorkerImplBase {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SearchService.class.getName());

  private Queue<SearchRequestContext> requestQueue;

  private ExecutorService pool;

  private Worker worker;

  /**
   * Constructor
   * @param numHandlers number of threads in the thread pool
   */
  SearchService(Worker worker, int numHandlers) {
    this.requestQueue =  new ArrayBlockingQueue<SearchRequestContext>(numHandlers);
    this.pool = Executors.newCachedThreadPool();
    this.worker = worker;
    for (int i = 0; i < numHandlers; i++) {
      pool.submit(new SearchRequestHandler(requestQueue));
    }
  }

  @Override
  public void echo(
      EchoRequest request,
      StreamObserver<EchoResponse> responseObserver) {
    LOG.info("echo from master: " + request.getMessage());
    EchoResponse response = EchoResponse.newBuilder().setMessage("welcome").build();
    LOG.info("echo to master: " + response.getMessage());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void search(
      SearchRequest request,
      StreamObserver<SearchResult> responseObserver) {
    LOG.info("Receive search request: " + request);
    boolean added = requestQueue.offer(new SearchRequestContext(request, responseObserver));
    while (!added) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
      added = requestQueue.offer(new SearchRequestContext(request, responseObserver));
    }
  }

  @Override
  public void shutdown(ShutdownRequest request,
      StreamObserver<ShutdownResponse> responseObserver) {
    // handle all search request before shutting down
    pool.shutdown();

    ShutdownResponse response;
    try {
      // shutdown the channel
      worker.shutdown();
      response = ShutdownResponse.newBuilder()
          .setStatus(ShutdownResponse.Status.SUCCESS)
          .setMessage("bye")
          .build();
    } catch (InterruptedException e) {
      response = ShutdownResponse.newBuilder()
          .setStatus(ShutdownResponse.Status.FAILURE)
          .setMessage(e.getMessage())
          .build();
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  static class SearchRequestContext {
    private SearchRequest request;
    private StreamObserver<SearchResult> responseObserver;

    SearchRequestContext(SearchRequest request, StreamObserver<SearchResult> responseObserver) {
      this.request = request;
      this.responseObserver = responseObserver;
    }

    SearchRequest getRequest() {
      return request;
    }

    StreamObserver<SearchResult> getResponseObserver() {
      return responseObserver;
    }
  }

}
