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

public class CustomCost implements RelOptCost {
    private final double rowCount, cpu, io;

    public CustomCost(double rowCount, double cpu, double io) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }

    @Override
    public boolean isLe(RelOptCost other) {
        return this.rowCount <= other.getRows();
    }

    @Override
    public boolean isLt(RelOptCost other) {
        return this.rowCount >= other.getRows();
    }

    @Override
    public boolean isInfinite() {
        return false;
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        return this.rowCount == other.getRows() && this.cpu == other.getCpu() && this.io == other.getIo();
    }

    @Override
    public boolean equals(RelOptCost other) {
        return this.rowCount == other.getRows() && this.cpu == other.getCpu() && this.io == other.getIo();
    }

    @Override
    public double getRows() { return rowCount; }

    @Override
    public double getCpu() { return cpu; }

    @Override
    public double getIo() { return io; }

    @Override
    public RelOptCost plus(RelOptCost other) {
        return new CustomCost(this.rowCount + other.getRows(), this.cpu + other.getCpu(), this.io + other.getIo());
    }

    public RelOptCost minus(RelOptCost other) {
        return new CustomCost(this.rowCount - other.getRows(), this.cpu - other.getCpu(), this.io - other.getIo());
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        return new CustomCost(this.rowCount * factor, this.cpu * factor, this.io * factor);
    }

    @Override
    public double divideBy(RelOptCost other) {
        if (other.getRows() == 0) {
            return this.rowCount; // Avoid division by zero
        }
        //return new CustomCost(this.cost / other.getRows());
        return this.rowCount / other.getRows();
    }
}

