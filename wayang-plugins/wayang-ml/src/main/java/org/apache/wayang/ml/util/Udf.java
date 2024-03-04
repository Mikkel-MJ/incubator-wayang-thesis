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

package org.apache.wayang.ml.util;

import org.apache.wayang.core.function.UDFComplexity;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.basic.operators.*;

public class Udf {

    public static UDFComplexity getComplexity(Operator operator) {
        if (operator instanceof ReduceByOperator) {
            return ((ReduceByOperator) operator).getReduceDescriptor().getUDFComplexity();
        }

        if (operator instanceof ReduceOperator) {
            return ((ReduceOperator) operator).getReduceDescriptor().getUDFComplexity();
        }

        if (operator instanceof CoGroupOperator) {
            return ((CoGroupOperator) operator).getKeyDescriptor0().getUDFComplexity();
        }

        if (operator instanceof FilterOperator) {
            return ((FilterOperator) operator).getPredicateDescriptor().getUDFComplexity();
        }

        if (operator instanceof FlatMapOperator) {
            return ((FlatMapOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof GlobalReduceOperator) {
            return ((GlobalReduceOperator) operator).getReduceDescriptor().getUDFComplexity();
        }
        if (operator instanceof GroupByOperator) {
            return ((GroupByOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        if (operator instanceof JoinOperator) {
            return ((JoinOperator) operator).getKeyDescriptor0().getUDFComplexity();
        }

        if (operator instanceof LoopOperator) {
            return ((LoopOperator) operator).getCriterionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MapOperator) {
            return ((MapOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MapPartitionsOperator) {
            return ((MapPartitionsOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MaterializedGroupByOperator) {
            return ((MaterializedGroupByOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        if (operator instanceof SortOperator) {
            return ((SortOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        return UDFComplexity.LINEAR;
    }
}
