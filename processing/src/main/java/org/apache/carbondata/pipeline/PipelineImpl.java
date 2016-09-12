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

package org.apache.carbondata.pipeline;

import java.util.LinkedList;
import java.util.List;

/**
 * A straight forward implementation by executing all steps sequentially.
 */
public class PipelineImpl implements Pipeline {

  private List<Step> steps;
  private PipelineContext context;

  public PipelineImpl(Step.Factory[] factories, PipelineContext context) {
    // use factory to initialize steps
    steps = new LinkedList<>();
    for (Step.Factory factory : factories) {
      steps.add(factory.create()); // TODO: steps should ensure correct order
    }
    this.context = context;
  }

  /**
   * trigger this pipeline to run
   */
  public void run() {
    // Trigger each step to work
    for (Step step : steps) {
      step.doWork(context);
    }
  }
}
