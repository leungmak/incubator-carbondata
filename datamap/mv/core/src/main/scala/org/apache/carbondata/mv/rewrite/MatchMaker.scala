/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class MatchPattern[MatchingPlan <: TreeNode[MatchingPlan]] extends Logging {
  
  def apply(subsumer: MatchingPlan,subsumee: MatchingPlan,compensation: Option[MatchingPlan],rewrite: QueryRewrite): Seq[MatchingPlan]
  
}

abstract class MatchMaker[MatchingPlan <: TreeNode[MatchingPlan]] {

  /** Define a sequence of rules, to be overridden by the implementation. */ 
  protected val patterns: Seq[MatchPattern[MatchingPlan]]
  
  def execute(subsumer: MatchingPlan,subsumee: MatchingPlan,compensation: Option[MatchingPlan],rewrite:QueryRewrite): Iterator[MatchingPlan] = {
    val iter = patterns.view.flatMap(_(subsumer, subsumee, compensation, rewrite)).toIterator
    iter

  }
}