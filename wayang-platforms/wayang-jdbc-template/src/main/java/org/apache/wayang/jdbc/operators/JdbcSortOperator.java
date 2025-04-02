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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import java.sql.Connection;

public abstract class JdbcSortOperator extends SortOperator<Record, Record> implements JdbcExecutionOperator {
    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcSortOperator(final SortOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        throw new UnsupportedOperationException("Jdbc sort operators are not currently supported");
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.sorts.load", this.getPlatform().getPlatformId());
    }
}
