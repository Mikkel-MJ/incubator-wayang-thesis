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

package org.apache.wayang.core.util;

import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.*;
import org.apache.commons.lang3.ArrayUtils;

public abstract class BinaryTree<T> {
    public T value;
    public BinaryTree<T> left;
    public BinaryTree<T> right;


    protected abstract BinaryTree<T> create();

    /*
     * Utility to define how you would display this
     * particular nodes values
     *
     * @return String
     */
    public String display() {
        return value.toString();
    }

    public BinaryTree<T> getLeft() {
        return left;
    }

    public BinaryTree<T> getRight() {
        return right;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        toStringHelper(this, "", sb);
        return sb.toString();
    }

    /**
     * Helper method to recursively build the string representation of the tree.
     */
    private void toStringHelper(BinaryTree<T> node, String indent, StringBuilder sb) {
        if (node == null) {
            return;
        }

        // Add the current node's value
        sb.append(indent).append(node.display()).append("\n");

        // If it's a leaf, no need to go further
        if (node.isLeaf()) {
            return;
        }

        // If there is a left child, print it with proper indentation
        if (node.getLeft() != null) {
            sb.append(indent).append("L--- ");
            toStringHelper(node.getLeft(), indent + "    ", sb);
        }

        // If there is a right child, print it with proper indentation
        if (node.getRight() != null) {
            sb.append(indent).append("R--- ");
            toStringHelper(node.getRight(), indent + "    ", sb);
        }
    }


    /*
     * Utility function for tree traversal
     * without return value. Can be used for
     * mutation.
     *
     * @param Consumer<BinaryTree<T>> func
     * @return void
     */
    public void traverse(Consumer<BinaryTree<T>> func) {
        func.accept(this);

        if (isLeaf()) {
            return;
        }

        if (left != null) {
            func.accept(left);
        }

        if (right != null) {
            func.accept(right);
        }
    }

    /*
     * Utility function for tree traversal
     * with return value.
     *
     * @param UnaryOperator<BinaryTree<T>> func
     * @return void
     */
    public BinaryTree<T> traverse(UnaryOperator<BinaryTree<T>> func) {
        if (isLeaf()) {
            return func.apply(this);
        }

        if (left != null) {
            left = func.apply(left);
        }

        if (right != null) {
            right = func.apply(right);
        }

        return func.apply(this);
    }

    /*
     * Utility function to rebalance the tree to a guaranteed
     * BinaryTree
     *
     * @return void
     */
    public void rebalance() {
        if (isLeaf()) {
            return;
        }

        if (left != null) {
            left.rebalance();
        }

        if (right != null) {
            right.rebalance();
        }

        if (left == null && right != null) {
            left = create();
        }

        if (left != null && right == null) {
            right = create();
        }
    }

    public boolean isLeaf() {
        return left == null && right == null;
    }

    public int size() {
        int size = 1;

        if (isLeaf()) {
            return 1;
        }

        if (left != null) {
            size += left.size();
        }

        if (right != null) {
            size += right.size();
        }

        return size;
    }
}
