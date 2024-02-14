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

import org.apache.wayang.core.plan.wayangplan.*;
import org.reflections.Reflections;
import java.util.Comparator;
import java.util.HashSet;

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

  /**
   * Lists all basic operators in Wayang
   */
  private final HashSet<Object> operators = new HashSet<>(
          new Reflections("org.apache.wayang.basic.operators").getSubTypesOf(OperatorBase.class)
  );


  /**
   * a 2D float array, with each float representing a probability from 0 to 1
   * The first column representing the current operator,
   * and the rows representing the probability to transition to a new operator.
   */
  //        source, filter, join, map, sink
  //source
  //filter
  //join
  //map
  //sink
  private final double[][] markovTransitionMatrix =
      {
              {0.00,0.33,0.33,0.33,0.00},
              {0.00,0.10,0.40,0.40,0.10},
              {0.00,0.40,0.10,0.40,0.10},
              {0.00,0.40,0.40,0.10,0.10},
              {0.00,0.00,0.00,0.00,0.00},
      };
}
