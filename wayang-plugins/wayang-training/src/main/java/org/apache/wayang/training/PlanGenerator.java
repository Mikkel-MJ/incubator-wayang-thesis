/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.training;

import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class PlanGenerator {

  public static void main(String[] args) {
    // Create Wayang Plans
    // 1. Args should take some input such as number of plans to generate, and
    // number of executionplans to generate

    System.out.println("HELLO WORLD");
  }

  // Should be able to generate a WayangPlan that matches a workload
  public WayangPlan generatePlan(UnarySource source) {
    Operator currentOperator = source;
    while (currentOperator instanceof UnarySink) {
      var nextOperator = generateNextOperator(currentOperator);

      currentOperator.connectTo(0, nextOperator, 0);
      currentOperator = nextOperator;

    }
    return null;
  }

  // should be able to give a operator back that can and make sense to connect to
  // the currentOperator
  private Operator generateNextOperator(Operator currentOperator) {
    return null;
  }

  private float[][] markovTransistionMatrix;

}
