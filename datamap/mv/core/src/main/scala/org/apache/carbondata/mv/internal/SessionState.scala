

package org.apache.carbondata.mv.internal

import org.apache.carbondata.mv.MQOSession
import org.apache.carbondata.mv.plans.modular.SimpleModularizer
import org.apache.carbondata.mv.plans.util.BirdcageOptimizer
import org.apache.carbondata.mv.rewrite.{DefaultMatchMaker, Navigator, QueryRewrite}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A class that holds all session-specific state in a given [[MQOSession]].
 */
private[mv] class SessionState(mqoSession: MQOSession) {

  // Note: These are all lazy vals because they depend on each other (e.g. conf) and we
  // want subclasses to override some of the fields. Otherwise, we would get a lot of NPEs.

  /**
   * Internal catalog for managing table and database states.
   */
  lazy val catalog = mqoSession.sharedState.summaryDatasetCatalog
    
//  lazy val catalog = new SessionCatalog(
//    mqoSession.sharedState.externalCatalog,
//    mqoSession.sharedState.globalTempViewManager,
//    functionResourceLoader,
//    functionRegistry,
//    conf,
//    newHadoopConf())
  
  /**
   * Modular query plan modularizer
   */
  lazy val modularizer = SimpleModularizer

  /**
   * Logical query plan optimizer.
   */
  lazy val optimizer = BirdcageOptimizer
  
  lazy val matcher = DefaultMatchMaker
  
  lazy val navigator: Navigator = new Navigator(catalog, mqoSession)


  def rewritePlan(plan: LogicalPlan): QueryRewrite = new QueryRewrite(mqoSession, plan)

}
