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

package org.apache.spark.sql

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier
import org.apache.carbondata.core.carbon.datastore.SegmentTaskIndexStore
import org.apache.carbondata.core.carbon.datastore.block.{BlockletInfos, TableBlockInfo}
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory
import org.apache.carbondata.hadoop.readsupport.impl.{DictionaryDecodedReadSupportImpl, RawDataReadSupport}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.scan.executor.QueryExecutorFactory
import org.apache.carbondata.scan.expression.Expression
import org.apache.carbondata.scan.filter.FilterExpressionProcessor
import org.apache.carbondata.scan.model.QueryModel
import org.apache.carbondata.spark.RawValue
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl
import org.apache.carbondata.spark.util.CarbonQueryUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, JobID}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext, _}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.util.SerializableConfiguration

class CarbonSparkPartition(rddId: Int, val idx: Int,
    val locations: Array[String],
    val tableBlockInfos: util.List[TableBlockInfo])
  extends Partition {

  override val index: Int = idx

  // val serializableHadoopSplit = new SerializableWritable[Array[String]](locations)
  override def hashCode(): Int = {
    41 * (41 + rddId) + idx
  }
}

/**
 * This RDD is used to perform query on CarbonData file. Before sending tasks to scan
 * CarbonData file, this RDD will leverage CarbonData's index information to do CarbonData file
 * level filtering in driver side.
 */
class CarbonScanRDD[V: ClassTag](
    @transient sc: SparkContext,
    columnProjection: Seq[Attribute],
    filterExpression: Expression,
    identifier: AbsoluteTableIdentifier,
    @transient carbonTable: CarbonTable)
  extends RDD[V](sc, Nil)
    with SparkHadoopMapReduceUtil
    with Logging {

  @transient private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }
  @transient private val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[Partition] = {
    val parallelism = sparkContext.defaultParallelism
    var noOfBlocks = 0
    var noOfNodes = 0
    var noOfTasks = 0

    //val jobContext = newJobContext(conf.value, jobId)
    val job = Job.getInstance(new Configuration())
    val statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val format = new CarbonInputFormat[V]
    CarbonInputFormat.setTablePath(job.getConfiguration, identifier.getTablePath)

    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)

    val result = new util.ArrayList[Partition](parallelism)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val segments = new SegmentStatusManager(identifier).getValidAndInvalidSegments
    // set filter resolver tree
    try {
      // before applying filter check whether segments are available in the table.
      if (!segments.getValidSegments.isEmpty) {
        //val carbonTable = CarbonInputFormat.getCarbonTable(conf.value)
        CarbonInputFormat.setCarbonTable(job.getConfiguration, carbonTable)
        CarbonInputFormat.setFilterPredicates(job.getConfiguration, filterExpression)
        CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, segments.getValidSegments)
        val projection = new CarbonProjection
        columnProjection.foreach { attr =>
          projection.addColumn(attr.asInstanceOf[AttributeReference].name)
        }
        CarbonInputFormat.setColumnProjection(projection, job.getConfiguration)

        //CarbonInputFormatUtil.processFilterExpression(filterExpression, carbonTable)
        //val filterResolver = FilterExpressionProcessor.getResolvedFilter(identifier, filterExpression)
//        queryModel.setInvalidSegmentIds(segments.getInvalidSegments)
//        queryModel.setFilterExpressionResolverTree(filterResolver)
        //CarbonInputFormat.setFilterPredicates(conf.value, filterResolver)
        SegmentTaskIndexStore.getInstance.removeTableBlocks(segments.getInvalidSegments, identifier)
      }
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        sys.error("Exception occurred in query execution :: " + e.getMessage)
    }
    // get splits
    val splits = format.getSplits(job)
    splits.asScala.map { split =>
      new SerializableWritable(split.asInstanceOf[CarbonInputSplit])
    }.zipWithIndex.map { case (split, index) =>
      new CarbonHadoopFSPartition(id, index, split)
    }.toArray

//    if (!splits.isEmpty) {
//      val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
//      val blockListTemp = carbonInputSplits.map(inputSplit =>
//        new TableBlockInfo(inputSplit.getPath.toString,
//          inputSplit.getStart, inputSplit.getSegmentId,
//          inputSplit.getLocations, inputSplit.getLength,
//          new BlockletInfos(inputSplit.getNumberOfBlocklets, 0, inputSplit.getNumberOfBlocklets)
//        )
//      )
//      val activeNodes = if (blockListTemp.nonEmpty) {
//        DistributionUtil.ensureExecutorsAndGetNodeList(blockListTemp.toArray, sparkContext)
//      } else {
//        Array[String]()
//      }
//
//      val blockList = CarbonLoaderUtil.distributeBlockLets(blockListTemp.asJava, parallelism)
//      if (blockList.asScala.nonEmpty) {
//        var statistic = new QueryStatistic()
//        // group blocks to nodes, tasks
//        val nodeBlockMapping =
//          CarbonLoaderUtil.nodeBlockTaskMapping(blockList, -1, parallelism,
//            activeNodes.toList.asJava)
//        statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis)
//        statisticRecorder.recordStatisticsForDriver(statistic, queryId)
//        statistic = new QueryStatistic()
//        var i = 0
//        // Create Spark Partition for each task and assign blocks
//        nodeBlockMapping.asScala.foreach { entry =>
//          entry._2.asScala.foreach { blocksPerTask => {
//            val tableBlockInfo = blocksPerTask.asScala.map(_.asInstanceOf[TableBlockInfo])
//            if (blocksPerTask.size() != 0) {
//              result
//                .add(new CarbonSparkPartition(id, i, Seq(entry._1).toArray, tableBlockInfo.asJava))
//              i += 1
//            }
//          }
//          }
//        }
//        noOfBlocks = blockList.size
//        noOfNodes = nodeBlockMapping.size
//        noOfTasks = result.size()
//        statistic.addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION,
//          System.currentTimeMillis)
//        statisticRecorder.recordStatisticsForDriver(statistic, queryId)
//        statisticRecorder.logStatisticsAsTableDriver()
//        result.asScala.foreach { r =>
//          val cp = r.asInstanceOf[CarbonSparkPartition]
//          logInfo(s"Node : " + cp.locations.toSeq.mkString(",")
//                  + ", No.Of Blocks : " + cp.tableBlockInfos.size())
//        }
//      }
//    }
//
//    logInfo(
//      s"""
//         | Identified no.of.blocks: $noOfBlocks,
//         | no.of.tasks: $noOfTasks,
//         | no.of.nodes: $noOfNodes,
//         | parallelism: $parallelism
//       """.stripMargin)

    //result.toArray(new Array[Partition](result.size()))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[V] = {

    val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
    val attempContext = newTaskAttemptContext(new Configuration(), attemptId)
    val job: Job = new Job(attempContext.getConfiguration)
    val format = new CarbonInputFormat[V]
    CarbonInputFormat.setCarbonReadSupport(classOf[RawDataReadSupport], attempContext.getConfiguration)
    CarbonInputFormat.setTablePath(attempContext.getConfiguration, identifier.getTablePath)
    CarbonInputFormat.setFilterPredicates(attempContext.getConfiguration, filterExpression)
    val projection = new CarbonProjection
    columnProjection.foreach { attr =>
      projection.addColumn(attr.asInstanceOf[AttributeReference].name)
    }
    CarbonInputFormat.setColumnProjection(projection, attempContext.getConfiguration)

    val inputSplit = split.asInstanceOf[CarbonHadoopFSPartition].carbonSplit.value
    val reader = format.createRecordReader(inputSplit, attempContext)
    reader.initialize(inputSplit, attempContext)

    val queryStartTime = System.currentTimeMillis

//    val carbonSparkPartition = split.asInstanceOf[CarbonSparkPartition]
//    if(!carbonSparkPartition.tableBlockInfos.isEmpty) {
//      queryModel.setQueryId(queryId + "_" + carbonSparkPartition.idx)
//      // fill table block info
//      queryModel.setTableBlockInf]
    // os(carbonSparkPartition.tableBlockInfos)
//      val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
//      if (null == carbonPropertiesFilePath) {
//        System.setProperty("carbon.properties.filepath",
//          System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
//        )
//      }
//    }

    new Iterator[V] {
      private[this] var havePair = false
      private[this] var finished = false

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            reader.close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val value: V = reader.getCurrentValue
        value
      }
    }
  }

  /**
   * Get the preferred locations where to launch this task.
   */
//  override def getPreferredLocations(split: Partition): Seq[String] = {
//    val theSplit = split.asInstanceOf[CarbonSparkPartition]
//    val firstOptionLocation = theSplit.locations.filter(_ != "localhost")
//    val tableBlocks = theSplit.tableBlockInfos
//    // node name and count mapping
//    val blockMap = new util.LinkedHashMap[String, Integer]()
//
//    tableBlocks.asScala.foreach(tableBlock => tableBlock.getLocations.foreach(
//      location => {
//        if (!firstOptionLocation.exists(location.equalsIgnoreCase)) {
//          val currentCount = blockMap.get(location)
//          if (currentCount == null) {
//            blockMap.put(location, 1)
//          } else {
//            blockMap.put(location, currentCount + 1)
//          }
//        }
//      }
//    )
//    )
//
//    val sortedList = blockMap.entrySet().asScala.toSeq.sortWith((nodeCount1, nodeCount2) => {
//      nodeCount1.getValue > nodeCount2.getValue
//    }
//    )
//
//    val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
//    firstOptionLocation ++ sortedNodesList
//  }
}
