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

package org.apache.wayang.jdbc.operators;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.wayang.jdbc.execution.DatabaseDescriptor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A Flink InputFormat that executes a SQL query using a JDBC connection and streams the results as Output objects.
 */
public class SqlResultInputFormat<Output> extends RichInputFormat<Output, GenericInputSplit> {

    private final DatabaseDescriptor descriptor;
    private final String sqlQuery;
    private final boolean needsTupleWrapping;

    private transient Connection connection;
    private transient SqlToStreamOperator.ResultSetIterator<Output> iterator;

    public SqlResultInputFormat(DatabaseDescriptor descriptor, String sqlQuery, boolean needsTupleWrapping) {
        this.descriptor = descriptor;
        this.sqlQuery = sqlQuery;
        this.needsTupleWrapping = needsTupleWrapping;
    }

    @Override
    public void configure(Configuration parameters) {
        // Optional: for config injection
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        try {
            this.connection = this.descriptor.createJdbcConnection();
            this.iterator = new SqlToStreamOperator.ResultSetIterator<>(connection, sqlQuery, this.needsTupleWrapping);
        } catch (Exception e) {
            throw new IOException("Failed to open JDBC connection or execute SQL query.", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !iterator.hasNext();
    }

    @Override
    public Output nextRecord(Output reuse) throws IOException {
        return iterator.next();
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException("Failed to close JDBC connection.", e);
        }
    }

    @Override
    public GenericInputSplit[] createInputSplits(int minNumSplits) {
        GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] splits) {
        return new DefaultInputSplitAssigner(splits);
    }

    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }
}
