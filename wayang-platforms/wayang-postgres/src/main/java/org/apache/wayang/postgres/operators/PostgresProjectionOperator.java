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

package org.apache.wayang.postgres.operators;

import java.sql.Connection;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;

/**
 * PostgreSQL implementation of the {@link FilterOperator}.
 */
public class PostgresProjectionOperator extends JdbcProjectionOperator implements PostgresExecutionOperator {
    
    public PostgresProjectionOperator(String... fieldNames) {
        super(fieldNames);
    }

    public PostgresProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public PostgresProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected PostgresProjectionOperator createCopy() {
        return new PostgresProjectionOperator(this);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        //Specify each column reference to its table
        return String.join(", ", this.getFunctionDescriptor().getFieldNames());
    }
}
