/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.calcite.rel;

import org.apache.calcite.plan.*;

public class CustomCostFactory implements RelOptCostFactory {
    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
        return new CustomCost(rowCount, cpu, io);
    }

    @Override
    public RelOptCost makeHugeCost() {
        return makeCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return makeCost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @Override
    public RelOptCost makeTinyCost() {
        return makeCost(1.0, 1.0, 0.0);
    }

    @Override
    public RelOptCost makeZeroCost() {
        return makeCost(0.0, 0.0, 0.0);
    }
}

