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

import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
/**
 * ValidationRule to forbid going to Postgres
 * when input has not been on Postgres before
 */
public class PostgresSourceValidationRule extends ValidationRule {
    /*
     * Index of platform choice for Postgres
     */
    private int postgresIndex = 5;

    public PostgresSourceValidationRule() {}

    public void validate(Float[][] choices, long[][][] indexes, TreeNode tree) {
        //Start at 1, 0th platform choice is for null operators
        for(int i = 1; i < choices.length; i++) {
            Float max = Arrays.stream(choices[i]).max(Comparator.naturalOrder()).orElse(Float.MIN_VALUE);

            //Check if Postgres is to be chosen
            if (choices[i][postgresIndex].equals(max)) {
                //Check if Postgres has been chosen in one of the preceeding inputs
                boolean isAllowed = true;

                Tuple<Optional<Long>, Optional<Long>> inputIndexes = getInputIndexes((long) i, indexes);

                float epsilon = Float.MIN_NORMAL;

                // Check if this has inputs, aka is not a source
                System.out.println("Postgres choice identified");
                if (inputIndexes.getField0().isPresent()) {
                    int leftIndex = inputIndexes.getField0().get().intValue();
                    System.out.println("Postgres choice hasLeft: " + leftIndex);

                    if (choices.length > leftIndex) {
                        float maxLeft = Arrays.stream(choices[leftIndex]).max(Comparator.naturalOrder()).orElse(Float.MIN_VALUE);

                        if(Math.abs(choices[leftIndex][postgresIndex] - maxLeft) >= epsilon) {
                            System.out.println("Postgres choice disallowed");
                            isAllowed = false;
                        } else {
                            System.out.println("Postgres choice allowed: " + choices[leftIndex][postgresIndex] + "!=" + maxLeft);
                        }
                    }
                }

                if (inputIndexes.getField1().isPresent()) {
                    int rightIndex = inputIndexes.getField1().get().intValue();
                    System.out.println("Postgres choice hasRight: " + rightIndex);

                    if (choices.length > rightIndex) {
                        float maxRight = Arrays.stream(choices[rightIndex]).max(Comparator.naturalOrder()).orElse(Float.MIN_VALUE);
                        if(Math.abs(choices[rightIndex][postgresIndex] - maxRight) >= epsilon) {
                            System.out.println("Postgres choice disallowed");
                            isAllowed = false;
                        } else {
                            System.out.println("Postgres choice allowed: " + choices[rightIndex][postgresIndex] + "!=" + maxRight);
                        }
                    }
                }

                //One of the inputs is not postgres, so postgres as platform choice is disallowed
                if (!isAllowed) {
                    for (int j = 0; j < choices[i].length; j++) {
                        if (max.equals(choices[i][j])) {
                            /*
                             * Set this choice to zero, identifying the platform
                             * choices later will take care of the rest
                             */
                            choices[i][j] = 0f;
                            break;
                        }
                    }
                }
            }
        }
    }

    /*
     * Helper to retrieve the input indexes from a given index
     */
    private Tuple<Optional<Long>, Optional<Long>> getInputIndexes(
        long index,
        long[][][] indexes
    ) {
        long[] flatIndexTree = Arrays.stream(indexes[0]).reduce(Longs::concat).orElseThrow();
        for (int i = 0; i < flatIndexTree.length; i+=3) {
            final long rootId = flatIndexTree[i];
            final long leftId = flatIndexTree[i+1];
            final long rightId = flatIndexTree[i+2];

            if (rootId == index) {
                Optional<Long> left = leftId == 0 ? Optional.empty() : Optional.of(leftId);
                Optional<Long> right = rightId == 0 ? Optional.empty() : Optional.of(rightId);

                return new Tuple<>(left, right);
            }
        }

        return new Tuple<>(Optional.empty(), Optional.empty());
    }
}
