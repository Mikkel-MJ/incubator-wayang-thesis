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

package org.apache.wayang.api.sql.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This {@link Operator} converts {@link StreamChannel}s to
 * {@link SqlQueryChannel}s.
 */
public class StreamToSqlOperator extends UnaryToUnaryOperator<Record, Record> implements JdbcExecutionOperator, JsonSerializable {
    protected StreamToSqlOperator(UnaryToUnaryOperator<Record, Record> that, final JavaPlatform javaPlatform) {
        super(that);
    }

    @Override
    public WayangJsonObj toJson() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJson'");
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createSqlClause'");
    }

    @Override
    public JdbcPlatformTemplate getPlatform() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPlatform'");
    }
}
