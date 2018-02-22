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


package org.apache.carbondata.mv.rewrite

//TODO: implement this to modularize DefaultMatchingFunctions
object MatchConditions {
//  def setupMatchConditions(subsumer: SparkyPlan, subsumee: SparkyPlan, compensation: SparkyPlan): MatchConditions = {
//    val isUniqueRmE = subsumer.children.filter{x => subsumee.children.count(_ == x) > 1}
//    val isUniqueEmR = subsumee.children.filter{x => subsumer.children.count(_ == x) > 1}
//    
//        val extrajoin = subsumer.children.filterNot { child => subsumee.children.contains(child) }
//        val rejoin = subsumee.children.filterNot { child => subsumer.children.contains(child) }
//        val rejoinOutputList = rejoin.flatMap(_.output)
//        
//        val isPredicateRmE = subsumer.predicateList.forall(expr => subsumee.predicateList.exists(_.semanticEquals(expr)))
//        val isPredicateEmdR = subsumee.predicateList.forall(expr => subsumer.predicateList.exists(_.semanticEquals(expr)) || Utils.isDerivable(expr,subsumer.outputList ++ rejoinOutputList,subsumee,subsumer,None))
//        val isOutputEdR = subsumee.outputList.forall(expr => Utils.isDerivable(expr,subsumer.outputList ++ rejoinOutputList,subsumee,subsumer,None))
//
//        if (extrajoin.isEmpty && isPredicateRmE && isPredicateEmdR && isOutputEdR)
//    null
//  }
}

class MatchConditions(flags: Long) {
  def hasFlag(flag: Long): Boolean = ???
  
}