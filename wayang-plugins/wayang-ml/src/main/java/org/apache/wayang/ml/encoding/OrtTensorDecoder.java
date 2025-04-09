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

package org.apache.wayang.ml.encoding;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.wayang.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class OrtTensorDecoder {
    private HashMap<Long, TreeNode> nodeToIDMap = new HashMap<>();

    //TODO: figure out output structure, from ml model
    /**
     * Decodes the output from a tree based NN model
     * @param mlOutput takes the out put from @
     */
    public TreeNode decode(Tuple<ArrayList<long[][]>,ArrayList<long[][]>> mlOutput){
        long[][] values = mlOutput.field0.get(0);
        long[][] indexedTree = mlOutput.field1.get(0);
        System.out.println("Index tree: " + Arrays.deepToString(indexedTree));
        long[] flatIndexTree = Arrays.stream(indexedTree).reduce(Longs::concat).orElseThrow();

        for (int j = 0; j < flatIndexTree.length; j++) {
            final long curID = flatIndexTree[j];
            System.out.println("Looking at ID " + curID);

            // Skip 0s
            if (curID == 0) {
                continue;
            }

            long[] value = Arrays.stream(values)
                    .flatMapToLong(arr -> LongStream.of(arr[(int) curID]))
                    .toArray();

            // Skip 0s
            if (LongStream.of(value).reduce(0l, Long::sum) == 0) {
                System.out.println("SKIPPING 0s for " + Arrays.toString(value));
                continue;
            }


            //set values
            //fetch l,r from map such that we can reference values.
            TreeNode curTreeNode = nodeToIDMap.containsKey(curID) ? nodeToIDMap.get(curID) : new TreeNode(value, null, null);

            // Skip Nulloperator
            /*
            if (curTreeNode.isNullOperator()) {
                System.out.println("SKIPPING Nulloperator for " + curTreeNode);
                continue;
            }*/

            System.out.println("Setting: " + Arrays.toString(value) + " for " + curTreeNode);
            curTreeNode.encoded = value;

            //TODO: The assumption that you can always look for 3 nodes
            //in a subtree doesn't hold anymore, it needs fixing
            if (flatIndexTree.length > j+1) {
                long lID   = flatIndexTree[j+1];

                if (nodeToIDMap.containsKey(lID)) {
                    TreeNode l = nodeToIDMap.get(lID);
                    curTreeNode.left = l;
                    nodeToIDMap.put(lID,l);
                    System.out.println("Setting left for " + lID + "with " + l);
                }

                if (flatIndexTree.length > j+2) {
                    long rID   = flatIndexTree[j+2];

                    if (nodeToIDMap.containsKey(rID)) {
                        TreeNode r = nodeToIDMap.get(rID);
                        curTreeNode.right = r;
                        nodeToIDMap.put(rID,r);
                        System.out.println("Setting right for " + rID + "with " + r);
                    }
                }
            }

            //put values back into map so we can look them up in next loop
            nodeToIDMap.put(curID,curTreeNode);
            System.out.println("Put " + curTreeNode + " into " + curID);
        }

        return this.nodeToIDMap.get(1L);
    }
}
