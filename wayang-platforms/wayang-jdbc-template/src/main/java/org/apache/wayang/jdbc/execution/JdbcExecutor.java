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

package org.apache.wayang.jdbc.execution;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.wayang.basic.function.JoinKeyDescriptor;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.channels.SqlQueryChannel.Instance;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcGlobalReduceOperator;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.JdbcReduceByOperator;
import org.apache.wayang.jdbc.operators.JdbcSortOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.operators.SqlToRddOperator;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    static private class AliasMap extends HashMap<Operator, String> {
        private int counter = 0;

        public String computeIfAbsent(final Operator operator) {
            return super.computeIfAbsent(operator, op -> "subquery" + counter++);
        }
    }

    public static String visitTask(final Operator operator, final Map<Operator, List<Operator>> edgeMap,
            final int subqueryCount) {
        return JdbcExecutor.visitTask(operator, edgeMap, 0, new AliasMap());
    }

    private static String visitTask(final Operator operator, final Map<Operator, List<Operator>> edgeMap,
            final int subqueryCount,
            final AliasMap aliasMap) {
        final List<Operator> nextOperators = edgeMap.get(operator);

        if (operator instanceof JdbcJoinOperator) {
            assert nextOperators.size() == 2
                    : "amount of next next operators in join operator was not two, got: " +
                            nextOperators.size();

            final JdbcJoinOperator join = (JdbcJoinOperator) operator;
            final JoinKeyDescriptor joinKeyDescriptor0 = (JoinKeyDescriptor) join.getKeyDescriptor0();
            final JoinKeyDescriptor joinKeyDescriptor1 = (JoinKeyDescriptor) join.getKeyDescriptor1();

            // we recursively visit left and right nodes and treat them as subqueries
            final String left = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1,
                    aliasMap);
            final String right = visitTask(nextOperators.get(1), edgeMap, subqueryCount + 1,
                    aliasMap);

            // create alias from inner join left & right table
            final String alias = aliasMap.computeIfAbsent(operator);
            final String leftAlias = "left" + alias;
            final String rightAlias = "right" + alias;

            // the left join key descriptor contains projections from the left table in a
            // join
            // likewise with the right key descriptor. So we need to make sure that
            // leftAlias + rightAlias match this expectation
            final String[] projections = Stream.concat(
                    joinKeyDescriptor0.getFieldNames().stream()
                            .map(field -> leftAlias + "." + field),
                    joinKeyDescriptor1.getFieldNames().stream()
                            .map(field -> rightAlias + "." + field))
                    .toArray(String[]::new);

            final String[] aliases = Stream.concat(
                    joinKeyDescriptor0.getAliases().stream(),
                    joinKeyDescriptor1.getAliases().stream())
                    .toArray(String[]::new);

            assert projections.length == aliases.length
                    : "Amount of projections did not match the amount of aliases.";

            final String[] aliasedProjections = new String[projections.length];

            for (int i = 0; i < aliases.length; i++) {
                aliasedProjections[i] = projections[i] + " AS " + aliases[i];
            }

            final String selectStatement = Arrays.stream(aliasedProjections)
                    .collect(Collectors.joining(","));

            final String condition;

            if (joinKeyDescriptor0.hasSqlImpl()) {
                final List<String> aliasedLeft = joinKeyDescriptor0.getFieldNames().stream()
                        .map(name -> leftAlias + "." + name).collect(Collectors.toList());
                final List<String> aliasedRight = joinKeyDescriptor1.getFieldNames().stream()
                        .map(name -> rightAlias + "." + name).collect(Collectors.toList());
                final List<String> aliasedFields = Stream
                        .concat(aliasedLeft.stream(), aliasedRight.stream())
                        .collect(Collectors.toList());

                condition = joinKeyDescriptor0.createSqlString(aliasedFields);
            } else {
                // setup join condition
                final String leftField = joinKeyDescriptor0.getFieldNames()
                        .get(joinKeyDescriptor0.getkeys().get(0));
                final String rightField = joinKeyDescriptor1.getFieldNames()
                        .get(joinKeyDescriptor1.getkeys().get(0));

                assert leftField != null : "Left join field in filter was null.";
                assert rightField != null : "Right join field in filter was null.";

                condition = String.format("%s.%s = %s.%s",
                        leftAlias, leftField,
                        rightAlias, rightField);
            }

            return "SELECT " + selectStatement + " FROM (" + left + ") AS left" + alias +
                    " INNER JOIN (" + right
                    + ") AS right"
                    + alias + "  ON " + condition;
        } else if (operator instanceof JdbcProjectionOperator) {
            final JdbcProjectionOperator<?, ?> projection = (JdbcProjectionOperator<?, ?>) operator;

            System.out.println("on project: " + operator);
            assert nextOperators.size() == 1
                    : "amount of next operators of projection operator was not one, got: "
                            + nextOperators.size();
            final String columns = projection.getFunctionDescriptor().getFieldNames().stream()
                    .collect(Collectors.joining(", "));

            final String input = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1, aliasMap);

            // handle aliases
            final String alias = aliasMap.computeIfAbsent(operator);

            final String returnStmnt = columns.length() == 0
                    ? input
                    : "SELECT " + columns + " FROM (" + input + ") AS " + alias;

            return returnStmnt;
        } else if (operator instanceof JdbcSortOperator) {
            final JdbcSortOperator sort = (JdbcSortOperator) operator;

            assert nextOperators.size() == 1
                    : "amount of next operators of filter operator was not one, got: "
                            + nextOperators.size();
            final String input = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1, aliasMap);
            final String alias = aliasMap.computeIfAbsent(operator);

            return "SELECT * FROM (" + input + ") AS " + alias + " WHERE "
                    + sort.getKeyDescriptor().getSqlImplementation();
        } else if (operator instanceof JdbcReduceByOperator) {
            final JdbcReduceByOperator reduceBy = (JdbcReduceByOperator) operator;
            final String input = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1, aliasMap);
            final String alias = aliasMap.computeIfAbsent(operator);

            return "SELECT * FROM (" + input + ") AS " + alias + " WHERE "
                    + reduceBy.getKeyDescriptor().getSqlImplementation();
        } else if (operator instanceof JdbcFilterOperator) {
            final JdbcFilterOperator filter = (JdbcFilterOperator) operator;

            assert nextOperators.size() == 1
                    : "amount of next operators of filter operator was not one, got: "
                            + nextOperators.size();
            final String input = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1, aliasMap);
            final String alias = aliasMap.computeIfAbsent(operator);
            final List<String> fields = filter.getPredicateDescriptor().fieldNames;
            final String condition = filter.getPredicateDescriptor().createSqlString.apply(fields);

            return "SELECT * FROM (" + input + ") AS " + alias + " WHERE "
                    + condition;
        } else if (operator instanceof JdbcGlobalReduceOperator) {
            final JdbcGlobalReduceOperator<?> reduce = (JdbcGlobalReduceOperator<?>) operator;

            assert nextOperators.size() == 1
                    : "amount of next operators of filter operator was not one, got: "
                            + nextOperators.size();
            final String input = visitTask(nextOperators.get(0), edgeMap, subqueryCount + 1, aliasMap);
            final String alias = aliasMap.computeIfAbsent(operator);

            return "SELECT " + reduce.getReduceDescriptor().getSqlImplementation() + " FROM (" + input
                    + ") AS "
                    + alias;
        } else if (operator instanceof JdbcTableSource) {
            final JdbcTableSource table = (JdbcTableSource) operator;

            assert nextOperators.size() == 0
                    : "amount of next operators of reduce operator was not zero, got: "
                            + nextOperators.size();
            final String alias = aliasMap.computeIfAbsent(operator);

            return "SELECT * FROM " + table.getTableName() + " AS " + alias;
        } else {
            throw new UnsupportedOperationException("Operator not supported in JDBC executor: " + operator);
        }
    }

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext,
            final ExecutionState executionState) {
        final Map<Operator, List<Operator>> operatorMap = stage.getAllTasks().stream()
                .collect(
                        Collectors.toMap(ExecutionTask::getOperator,
                                task -> stage.getPreceedingTasks(task).stream()
                                        .map(ExecutionTask::getOperator)
                                        .collect(Collectors.toList()),
                                (a, b) -> a));

        final String query = stage.getTerminalTasks().stream()
                .map(task -> visitTask(task.getOperator(), operatorMap, 0, new AliasMap()))
                .findAny()
                .orElseThrow(() -> new WayangException("Could not produce Sql in JdbcExecutor."));
        
        final List<ExecutionTask> allTasksWithTableSources = Arrays
                .stream(stage.getAllTasks().toArray(ExecutionTask[]::new))
                .collect(Collectors.toList());

        final Stream<ExecutionTask> allBoundaryOperators = allTasksWithTableSources.stream()
                .filter(task -> task.getOutputChannel(0)
                        .getConsumers()
                        .stream()
                        .anyMatch(consumer -> consumer
                                .getOperator() instanceof SqlToStreamOperator
                                || consumer.getOperator() instanceof SqlToRddOperator));

        final Collection<Instance> outBoundChannels = allBoundaryOperators
                .map(task -> this.instantiateOutboundChannel(task, optimizationContext))
                .collect(Collectors.toList());

        assert outBoundChannels.size() <= 1
                : "Only one boundary operator is allowed per execution stage, but found "
                        + outBoundChannels.size();

        // set the string query generated above to each channel
        outBoundChannels.forEach(chann -> {
            chann.setSqlQuery(noAliasQuery);
            executionState.register(chann); // register at this execution stage so it gets executed
        });
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (final SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                whose outbound {@link SqlQueryChannel} should be
     *                            instantiated
     * @param optimizationContext provides information about the
     *                            {@link ExecutionTask}
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
            final OptimizationContext optimizationContext) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        final SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext
                .getOperatorContext(task.getOperator());
        return outputChannel.createInstance(this, operatorContext, 0);
    }
}