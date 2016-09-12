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
 *//*
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
package org.apache.carbondata.pipeline.load

import org.apache.carbondata.pipeline.Pipeline
import org.apache.carbondata.pipeline.PipelineContext
import org.apache.carbondata.pipeline.PipelineImpl
import org.apache.carbondata.pipeline.Step

/**
  * This is the main pipeline for load data into CarbonData.
  * This should be called in LOAD DATA SQL statement.
  */
object DataLoadPipeline {
  def main(args: Array[String]) {
    new DataLoadPipeline().run()
  }
}

class DataLoadPipeline() extends Pipeline {
  // run in 2 steps
  // step1: generate dictionary  (first scan)
  // step2: do actual data run (second scan)
  val factories = Array[Step.Factory](new DictGenStepFactory, new LoadStepFactory)
  val context = new PipelineContext
  // add all encoding and MDK order based on create table statement from user
  context.put("encoding", new Any)
  val pipeline = new PipelineImpl(factories, context)

  def run() {
    pipeline.run()
  }
}