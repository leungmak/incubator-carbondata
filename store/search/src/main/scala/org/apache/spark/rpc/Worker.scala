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

package org.apache.spark.rpc

import java.io.IOException
import java.net.InetAddress

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.search.{RegisterWorkerRequest, RegisterWorkerResponse, Searcher}
import org.apache.spark.util.ThreadUtils

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory

@InterfaceAudience.Internal
object Worker {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private val DEFAULT_PORT = 10021

  def init(masterHostname: String, masterPort: Int): Unit = {
    LOG.info(s"initializing worker ${InetAddress.getLocalHost.getHostName}:$DEFAULT_PORT")
    LOG.info("starting service...")
    startService()
    LOG.info(s"registering to master $masterHostname:$masterPort")
    val workerId = registerToMaster(masterHostname, masterPort)
    LOG.info(s"worker registered to master, workerId: $workerId")
  }

  private def startService(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        val hostname = InetAddress.getLocalHost.getHostName
        val conf = new SparkConf()
        val config = RpcEnvConfig(
          conf, s"worker-$hostname", hostname, "", DEFAULT_PORT,
          new SecurityManager(conf), clientMode = false)
        val rpcEnv: RpcEnv = new NettyRpcEnvFactory().create(config)
        val searchEndpoint: RpcEndpoint = new Searcher(rpcEnv)
        rpcEnv.setupEndpoint("search-service", searchEndpoint)
        LOG.info("search-service started")
        rpcEnv.awaitTermination()
      }
    }).start()
  }

  private def registerToMaster(masterHostname: String, masterPort: Int): String = {
    LOG.info(s"trying to register to master $masterHostname:$masterPort")
    val conf = new SparkConf()
    val config = RpcEnvConfig(conf, "registry-client", masterHostname, "", masterPort,
      new SecurityManager(conf), clientMode = true)
    val rpcEnv: RpcEnv = new NettyRpcEnvFactory().create(config)

    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(
      RpcAddress(masterHostname, masterPort), "registry-service")
    val cores = Runtime.getRuntime.availableProcessors()

    val hostname = InetAddress.getLocalHost.getHostName
    val request = RegisterWorkerRequest(hostname, DEFAULT_PORT, cores)
    val future = endPointRef.ask[RegisterWorkerResponse](request)
    ThreadUtils.awaitResult(future, Duration.apply("10s"))
    future.value match {
      case Some(result) =>
        result match {
          case Success(response) =>
            LOG.info("worker registered")
            response.workerId
          case Failure(throwable) =>
            LOG.error(s"worker failed to registered: $throwable")
            throw new IOException(throwable.getMessage)
        }
      case None =>
        LOG.error("worker register timeout")
        throw new ExecutionTimeoutException
    }
  }
}
