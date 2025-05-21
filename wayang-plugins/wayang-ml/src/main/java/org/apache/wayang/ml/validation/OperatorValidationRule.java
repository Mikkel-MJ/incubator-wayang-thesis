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

package org.apache.wayang.ml.validation;

import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.postgres.operators.PostgresTableSource;

import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
/**
 * ValidationRule to forbid certain platforms
 * when an operator doesn't exist for that platform
 */
public class OperatorValidationRule extends ValidationRule {

    private int postgresIndex = 5;

    public OperatorValidationRule() {}

    public void validate(Float[][] choices, long[][][] indexes, TreeNode tree) {
        //Start at 1, 0th platform choice is for null operators
        for(int i = 1; i < choices.length; i++) {
            TreeNode node = (TreeNode) tree.getNode(i);

            if (node != null && !node.isNullOperator()) {

                //Prevent TextFileSources from being in postgres
                if (node.operator instanceof TextFileSource) {
                    choices[i][postgresIndex] = 0f;
                }

                //Prevent TextFileSources from being outside of postgres
                if (node.operator instanceof PostgresTableSource) {
                    choices[i][postgresIndex] = Float.MAX_VALUE;
                }
            }
        }
    }

}
