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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;

public class TreeNode {
    public long[] encoded;
    public TreeNode left;
    public TreeNode right;
    public boolean isRoot;
    //private static Pattern pattern = Pattern.compile("\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))", Pattern.CASE_INSENSITIVE);
    private static Pattern pattern = Pattern.compile("\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<children>(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))|\\)*)", Pattern.CASE_INSENSITIVE);

    public TreeNode() { }

    public TreeNode(long[] encoded, TreeNode left, TreeNode right) {
        this.encoded = encoded;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        String encodedString = Arrays.toString(encoded).replace("[", "(").replace("]", ")").replaceAll("\\s+", "");

        if (left == null && right == null) {
            return '(' + encodedString + ",)";
        }

        String leftString = "";
        String rightString = "";

        if (left == null) {
            leftString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
        } else {
            leftString = left.toString();
        }

        if (right == null) {
            rightString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
        } else {
            rightString = right.toString();
        }

        return "(" + encodedString + "," + leftString + "," + rightString + ")";
    }

    public static TreeNode fromString(String encoded) {
        TreeNode result = new TreeNode();
        Matcher matcher = pattern.matcher(encoded);
        String value = "";

        if (!matcher.find()) {
            return null;
        }

        value = matcher.group("value");
        String left = matcher.group("left");
        String right = matcher.group("right");
        Long[] encodedLongs = Stream.of(value.split(","))
            .map(val -> Long.valueOf(val.replaceAll("\\s+","")))
            .collect(Collectors.toList()).toArray(Long[]::new);
        result.encoded = ArrayUtils.toPrimitive(encodedLongs);

        if (left != null) {
            result.left = TreeNode.fromString(left);
        }

        if (right != null) {
            result.right = TreeNode.fromString(right);
        }

        return result;
    }

    public TreeNode withIdsFrom(TreeNode node) {
        this.encoded[0] = node.encoded[0];

        if (this.left != null && node.left != null) {
            this.left = left.withIdsFrom(node.left);
        }

        if (this.right != null && node.right != null) {
            this.right = right.withIdsFrom(node.right);
        }

        return this;
    }

    public TreeNode withPlatformChoicesFrom(TreeNode node) {
        if (this.encoded != null) {
            assert node.encoded != null;
        }

        if (node.encoded == null) {
            assert this.encoded != null;
            //return this;
        }

        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();
        HashMap<String, Integer> operatorMappings = OneHotMappings.getInstance().getOperatorMapping();
        int platformPosition = -1;
        System.out.println("Encoding while choices: " + Arrays.toString(node.encoded));
        platformPosition = ArrayUtils.indexOf(node.encoded, 1);
        String platform = "";

        assert platformPosition >= 0;

        for (Map.Entry<String, Integer> pair : platformMappings.entrySet()) {
            if (pair.getValue() == platformPosition) {
                platform = pair.getKey();
            }
        }

        assert platform != "";

        System.out.println("Chose platform: " + platform);

        int operatorsCount = operatorMappings.size();
        this.encoded[operatorsCount + platformPosition] = 1;

        if (this.left != null) {
            assert node.left != null;
            this.left = left.withPlatformChoicesFrom(node.left);
        }

        if (this.right != null) {
            assert node.right != null;
            this.right = right.withPlatformChoicesFrom(node.right);
        }

        /*
        if (this.left != null && node.left != null) {
            this.left = left.withPlatformChoicesFrom(node.left);
        }*/

        /*
        if (this.right != null && node.right != null) {
            this.right = right.withPlatformChoicesFrom(node.right);
        }*/

        return this;
    }

    public void softmax() {
        Set<Integer> disallowed = Set.of(1, 4);

        if (this.encoded == null) {
            return;
        }

        final long maxValue = Arrays.stream(this.encoded).max().getAsLong();
        long[] values = Arrays.stream(this.encoded).map(value -> value == maxValue ? 1 : 0).toArray();

        /*
        for (int i = 0; i < values.length; i++) {
            if (values[i] == 1 && disallowed.contains(i)) {
                this.encoded[i] = 0;
                this.softmax();

                return;
            }
        }*/

        this.encoded = values;

        if (this.left != null) {
            left.softmax();
        }

        if (this.right != null) {
            right.softmax();
        }
    }

    public boolean isLeaf() {
        return this.left == null && this.right == null;
    }


    public int getTreeSize() {
        int size = 1;

        if (this.left != null) {
            size += this.left.getTreeSize();
        }

        if (this.right != null) {
            size += this.right.getTreeSize();
        }

        if (this.right != null && this.left == null) {
            size += 1;
        }

        if (this.right == null && this.left != null) {
            size += 1;
        }

        return size;
    }

}
