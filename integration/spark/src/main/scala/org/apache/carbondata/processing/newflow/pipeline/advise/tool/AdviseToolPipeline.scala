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
package org.apache.carbondata.processing.newflow.pipeline.advise.tool

import org.apache.carbondata.processing.newflow.pipeline.Pipeline
import org.apache.carbondata.processing.newflow.pipeline.PipelineContext
import org.apache.carbondata.processing.newflow.pipeline.PipelineImpl
import org.apache.carbondata.processing.newflow.pipeline.Step
import org.apache.carbondata.processing.newflow.pipeline.advise.AdviseIndexStepFactory
import org.apache.carbondata.processing.newflow.pipeline.load.DictGenStepFactory

/**
  * External Advise Tool
  */
object AdviseToolPipeline {
  def main(args: Array[String]) {
    new AdviseToolPipeline().run()
  }
}

class AdviseToolPipeline() extends Pipeline {
  // run in 2 steps
  // step1: analyzer data (first scan)
  // step2: advise the encoding
  val factories = Array[Step.Factory](new DictGenStepFactory, new AdviseIndexStepFactory)
  val context = new PipelineContext
  val pipeline = new PipelineImpl(factories, context)

  def run() {
    // Run this pipeline and output the advise
    pipeline.run()
  }
}