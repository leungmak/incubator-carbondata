package org.apache.carbondata.mv.rewrite

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.MQOSession
import org.apache.carbondata.mv.plans.modular.ModularPlan

/**
 * The primary workflow for rewriting relational queries using Spark libraries.  Designed to allow easy
 * access to the intermediate phases of query rewrite for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryRewrite private (
    mqoSession: MQOSession, 
    logical: LogicalPlan,
    nextSubqueryId: AtomicLong) {
  self =>
  
  def this(mqoSession: MQOSession, logical: LogicalPlan) =
    this(mqoSession, logical, new AtomicLong(0))
    
  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

  lazy val optimizedPlan: LogicalPlan = mqoSession.sessionState.optimizer.execute(logical)

  lazy val modularPlan: ModularPlan = mqoSession.sessionState.modularizer.modularize(optimizedPlan).next().harmonized

  lazy val withSummaryData: ModularPlan = mqoSession.sessionState.navigator.rewriteWithSummaryDatasets(modularPlan,self)

  lazy val toCompactSQL: String = withSummaryData.asCompactSQL
  
  lazy val toOneLineSQL: String = withSummaryData.asOneLineSQL
}