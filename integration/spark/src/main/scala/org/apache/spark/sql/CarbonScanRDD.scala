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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier
import org.apache.carbondata.core.carbon.datastore.SegmentTaskIndexStore
import org.apache.carbondata.core.carbon.datastore.block.Distributable
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory
import org.apache.carbondata.hadoop.readsupport.impl.RawDataReadSupport
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit, CarbonMultiBlockSplit, CarbonProjection}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.scan.expression.Expression
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobID}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext, _}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class CarbonSparkPartition(
    val rddId: Int,
    val idx: Int,
    // TODO: following two fields can be replaced by CarbonInputSplit
//    @transient val locations: Array[String],
//    @transient val tableBlockInfos: util.List[TableBlockInfo],
    @transient val multiBlockSplit: CarbonMultiBlockSplit)
  extends Partition {

  val split = new SerializableWritable[CarbonMultiBlockSplit](multiBlockSplit)

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
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

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient private val jobId = new JobID(jobTrackerId, id)
  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def getPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val format = prepareInputFormatForDriver(job.getConfiguration)

    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)

    // get splits
    val splits = format.getSplits(job)
    val result = distributeSplits(splits)
    result
  }

  private def distributeSplits(splits: util.List[InputSplit]): Array[Partition] = {
    // this function distributes the split based on following logic:
    // 1. based on data locality, to make split balanced on all available nodes
    // 2. if the number of split for one

    var statistic = new QueryStatistic()
    val statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val parallelism = sparkContext.defaultParallelism
    val result = new util.ArrayList[Partition](parallelism)
    var noOfBlocks = 0
    var noOfNodes = 0
    var noOfTasks = 0

    if (!splits.isEmpty) {
      // create a list of block based on split
      val blockList = splits.asScala.map(_.asInstanceOf[Distributable])
//      val blockList = carbonInputSplits.map { inputSplit =>
//        new TableBlockInfo(inputSplit.getPath.toString,
//          inputSplit.getStart, inputSplit.getSegmentId,
//          inputSplit.getLocations, inputSplit.getLength,
//          new BlockletInfos(inputSplit.getNumberOfBlocklets, 0, inputSplit.getNumberOfBlocklets)
//        )
//      }.map(_.asInstanceOf[Distributable])

      // get the list of executors and map blocks to executors based on locality
      val activeNodes = DistributionUtil.ensureExecutorsAndGetNodeList(blockList, sparkContext)
      val nodeBlockMapping = CarbonLoaderUtil.nodeBlockTaskMapping(blockList.asJava, -1,
        parallelism, activeNodes.toList.asJava)

      statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis)
      statisticRecorder.recordStatisticsForDriver(statistic, queryId)
      statistic = new QueryStatistic()

      var i = 0
      // Create Spark Partition for each task and assign blocks
      nodeBlockMapping.asScala.foreach { case (node, blockList) =>
        blockList.asScala.foreach { blocksPerTask =>
          val splits = blocksPerTask.asScala.map(_.asInstanceOf[CarbonInputSplit])
          if (blocksPerTask.size() != 0) {
            val multiBlockSplit = new CarbonMultiBlockSplit(splits.asJava, node)
            val partition = new CarbonSparkPartition(id, i, multiBlockSplit)
            result.add(partition)
            i += 1
          }
        }
      }

      noOfBlocks = splits.size
      noOfNodes = nodeBlockMapping.size
      noOfTasks = result.size()

      statistic = new QueryStatistic()
      statistic.addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION,
        System.currentTimeMillis)
      statisticRecorder.recordStatisticsForDriver(statistic, queryId)
      statisticRecorder.logStatisticsAsTableDriver()
    }
    logInfo(
      s"""
         | Identified no.of.blocks: $noOfBlocks,
         | no.of.tasks: $noOfTasks,
         | no.of.nodes: $noOfNodes,
         | parallelism: $parallelism
       """.stripMargin)
    result.toArray(new Array[Partition](result.size()))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[V] = {
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
      )
    }

    val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
    val attemptContext = newTaskAttemptContext(new Configuration(), attemptId)
    val format = prepareInputFormatForExecutor(attemptContext.getConfiguration)
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    val reader = format.createRecordReader(inputSplit, attemptContext)
    reader.initialize(inputSplit, attemptContext)

    val queryStartTime = System.currentTimeMillis

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

  private def prepareInputFormatForDriver(conf: Configuration): CarbonInputFormat[V] = {
    val format = new CarbonInputFormat[V]
    CarbonInputFormat.setTablePath(conf, identifier.getTablePath)
    CarbonInputFormat.setCarbonTable(conf, carbonTable)
    CarbonInputFormat.setFilterPredicates(conf, filterExpression)
    val projection = new CarbonProjection
    columnProjection.foreach { attr =>
      projection.addColumn(attr.asInstanceOf[AttributeReference].name)
    }
    CarbonInputFormat.setColumnProjection(conf, projection)

    val segments = new SegmentStatusManager(identifier).getValidAndInvalidSegments
    if (!segments.getValidSegments.isEmpty) {
      CarbonInputFormat.setSegmentsToAccess(conf, segments.getValidSegments)
      if (!segments.getInvalidSegments.isEmpty) {
        SegmentTaskIndexStore.getInstance
            .removeTableBlocks(segments.getInvalidSegments, identifier)
      }
    }
    format
  }

  private def prepareInputFormatForExecutor(conf: Configuration): CarbonInputFormat[V] = {
    val format = new CarbonInputFormat[V]
    CarbonInputFormat.setCarbonReadSupport(classOf[RawDataReadSupport], conf)
    CarbonInputFormat.setTablePath(conf, identifier.getTablePath)
    CarbonInputFormat.setFilterPredicates(conf, filterExpression)
    val projection = new CarbonProjection
    columnProjection.foreach { attr =>
      projection.addColumn(attr.asInstanceOf[AttributeReference].name)
    }
    CarbonInputFormat.setColumnProjection(conf, projection)
    format
  }

  /**
   * Get the preferred locations where to launch this task.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    val firstOptionLocation = theSplit.split.value.getLocations.filter(_ != "localhost")
    firstOptionLocation
    // node name and count mapping
//    val blockMap = new util.LinkedHashMap[String, Integer]()
//    tableBlocks.asScala.foreach { tableBlock =>
//      tableBlock.getLocations.foreach { location =>
//        if (!firstOptionLocation.exists(location.equalsIgnoreCase)) {
//          val currentCount = blockMap.get(location)
//          if (currentCount == null) {
//            blockMap.put(location, 1)
//          } else {
//            blockMap.put(location, currentCount + 1)
//          }
//        }
//      }
//    }
//
//    val sortedList = blockMap.entrySet().asScala.toSeq.sortWith((nodeCount1, nodeCount2) => {
//      nodeCount1.getValue > nodeCount2.getValue
//    })
//
//    val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
//    firstOptionLocation ++ sortedNodesList
  }
}
