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

package org.apache.carbondata.mv.tool

import java.io._
import java.text.SimpleDateFormat
import java.util.zip.GZIPInputStream
import java.util.{Date, TimeZone}

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.mv.dsl.plans._
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor


abstract class MVToolBase extends Logging {

  val mvFilePath = getMVFilePath

  //  val preparedQueryLogEntry = """^[{]\"entryType\":\"prepared_query\",.*?,\"sql\":\"(.*?)\",.*?,\"orgId\":\"(.*?)\".*?[}]$""".r

  class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
    private var line: String = _   
    advance()
    
    override def hasNext() = line ne null
    override def next() = { val retval = line; advance(); retval }
    
    private[this] def advance() = {
      try {
        line = reader.readLine()
      } catch { case ioe: IOException => }
      
      if (line == null && reader != null)  {
        try {
          reader.close
        } catch { case ioe: IOException => }
      } 
    }
  }
 
  object GzFileIterator {
    def apply(file: File, encoding: String) = {
      new BufferedReaderIterator(
          new BufferedReader(
              new InputStreamReader(
                  new GZIPInputStream(
                      new FileInputStream(file)), encoding)))
    }
  }
  
  def YML_DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
  val ONE_DAY_IN_MILLS: Long = 24 * 60 * 60 * 1000

  def end = new Date().getTime()
  def backwardDays = 7
  
  def batchQueries(spark: SparkSession, qIterator: Iterator[String], qLogEntry: Regex, pBatch: mutable.ArrayBuffer[ModularPlan]) = {
    for (query <- qIterator) {
      query match {
        case qLogEntry(sqlstmt) =>
            val analyzed = spark.sql(sqlstmt).queryExecution.analyzed
            
            // check if the plan is well-formed
            if (analyzed.resolved && !(analyzed.missingInput.nonEmpty && analyzed.children.nonEmpty)) {
              // please see the comment on preHarmonized of ModularPlan for the assumption of the form of queries 
              // for harmonization.  We assume queries in processing conform to the form.  If not, customize 
              // preHarmonized so that, for each query, the fact table is at the front of the FROM clause of the query 
              // (similar method to canonicalizeJoinOrderIfFeasible of DefaultCommonSubexpressionManager).
              Try(analyzed.optimize.modularize.harmonized) match {
                case Success(m) => pBatch += m
                case Failure(e) => logInfo("throw away query that does not have modular plan: " + sqlstmt)
              }
            }
            else logInfo("throw away ill-formed query: " + sqlstmt)
        case _ =>
          }        
    }
  }   

  def createQueryBatchs(spark: SparkSession, qbPreprocessor: QueryBatchPreprocessor, queryLogFilePathPattern: String, queryLogEntryPattern: String): Iterator[(Option[Signature], Seq[(ModularPlan,Int)])] = {
    
    val sdf = new SimpleDateFormat(YML_DEFAULT_DATE_FORMAT)
    sdf.setTimeZone(TimeZone.getTimeZone("UtC"))
    
    val planBatch = mutable.ArrayBuffer[ModularPlan]()
    
    for (a <- 1 to backwardDays) {
      val d = sdf.format(end - (a * ONE_DAY_IN_MILLS))
      val queryLoggingFile = queryLogFilePathPattern.replace("%d", d)
      val queryLogEntry = queryLogEntryPattern.r
      val iterator = GzFileIterator(new File(queryLoggingFile), "UTF-8")
      
      batchQueries(spark, iterator, queryLogEntry, planBatch)
    }
    
    val preprocessedBatch = qbPreprocessor.preprocess(planBatch.map(plan => (plan, 1)))
    preprocessedBatch.groupBy(_._1.signature).toIterator
  } 
  
  def adviseMVs(spark: SparkSession, qbPreprocessor: QueryBatchPreprocessor, csemanager: CommonSubexpressionManager, queryLogFilePathPattern: String, queryLogEntryPattern: String) {
    
//    CommonSubexpressionRuleEngine.tableCluster.set(QueryBatchRuleEngine.getTableCluster())
    // val conf = new SQLConf().copy(SQLConf.CBO_ENABLED -> true)
          
    val mvWriter = new PrintWriter(new File(mvFilePath))
    

    for ((signature, batchBySignature) <- createQueryBatchs(spark,qbPreprocessor, queryLogFilePathPattern,queryLogEntryPattern)) {
      val cses = csemanager.execute(batchBySignature)
      cses.foreach { case (cse, freq) => mvWriter.println(s"${cse.asCompactSQL};\n") }
    }
    mvWriter.close()
  }

  protected def getMVFilePath: String
  

}

