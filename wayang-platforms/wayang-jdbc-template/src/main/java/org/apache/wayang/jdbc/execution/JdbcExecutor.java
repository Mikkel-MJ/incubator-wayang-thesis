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

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.channels.SqlQueryChannel.Instance;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcGlobalReduceOperator;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    protected class ExecutionTaskOrderingComparator implements Comparator<ExecutionTask> {
        final Map<ExecutionTask, Set<ExecutionTask>> ordering;

        /**
         * A comparator that compares two {@link ExecutionTask}s based on whether or not
         * they are reachable from one or the other.
         *
         * @param isReachableMap see {@link ExecutionStage#canReachMap()}.
         */
        ExecutionTaskOrderingComparator(final Map<ExecutionTask, Set<ExecutionTask>> isReachableMap) {
            this.ordering = isReachableMap;
        }

        @Override
        public int compare(final ExecutionTask arg0, final ExecutionTask arg1) {
            if (ordering.get(arg0).contains(arg1))
                return 1;
            if (ordering.get(arg1).contains(arg0))
                return -1;
            return -1;
        }
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext,
            final ExecutionState executionState) {
        final Collection<ExecutionTask> startTasks = stage.getStartTasks();

        // order all tasks by whether or not a given task is reachable from another
        // i have to cast it to a list here otherwise java wont maintain ordering
        final List<ExecutionTask> allTasksWithTableSources = Arrays
                .stream(stage.getAllTasks().toArray(ExecutionTask[]::new))
                .sorted(new ExecutionTaskOrderingComparator(stage.canReachMap()))
                .collect(Collectors.toList());

        final List<ExecutionTask> allTasks = allTasksWithTableSources.stream()
                .filter(task -> !(task.getOperator() instanceof TableSource))
                .sorted(new ExecutionTaskOrderingComparator(stage.canReachMap()))
                .collect(Collectors.toList());

        assert startTasks.stream().allMatch(task -> task.getOperator() instanceof TableSource);

        final Stream<TableSource> tableSources = startTasks.stream()
                .map(task -> (TableSource) task.getOperator());
        final Collection<ExecutionTask> filterTasks = allTasks.stream()
                .filter(task -> task.getOperator() instanceof JdbcFilterOperator)
                .collect(Collectors.toList());
        final Collection<ExecutionTask> projectionTasks = allTasks.stream()
                .filter(task -> task.getOperator() instanceof JdbcProjectionOperator)
                .collect(Collectors.toList());
        final Collection<ExecutionTask> joinTasks = allTasks.stream()
                .filter(task -> task.getOperator() instanceof JdbcJoinOperator)
                .collect(Collectors.toList());

        // Create the SQL query.
        final Stream<String> tableNames = tableSources.map(this::getSqlClause);
        final String joinedTableNames = tableNames.collect(Collectors.joining(","));

        final Collection<String> conditions = filterTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());

        // mapping that asks whether this is a projection that comes after a table
        // source, if so,
        // then we will use it in the select statement
        final Map<ExecutionTask, Boolean> taskInputIsTableMap = projectionTasks.stream()
                .collect(Collectors.toMap(
                        task -> task,
                        task -> !(task.getInputChannel(0).getProducerOperator() instanceof JdbcJoinOperator)));

        // collect projections necessary in select statement
        final String collectedProjections = projectionTasks.stream()
                .filter(taskInputIsTableMap::get)
                .map(task -> task.getOperator())
                .map(this::getSqlClause)
                .collect(Collectors.joining(","));

        final String projection = collectedProjections.equals("") ? joinedTableNames : collectedProjections;

        final Collection<String> joins = joinTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());

        // build the select statement by looking at the last operator in the boundary
        // pipeline
        final List<ExecutionTask> boundaryPipeline = this
                .dfsProjectionPipeline(stage.getTerminalTasks().iterator().next(), stage);

        // search each of the boundary operators for distinct sql clauses
        /*
         * final List<String> distinctSqlClauses = boundaryPipeline.stream()
         * .map(boundary -> boundary.getOperator())
         * .map(op -> this.getSqlClause(op).split(","))
         * .flatMap(Arrays::stream)
         * .distinct()
         * .collect(Collectors.toList());
         */

        // search each of the boundary operators for distinct sql clauses

        // TODO: find a way of making sure that we dont select on too many tables
        // in the sql selection statement
        final List<String> nonDistinctSqlClauses = boundaryPipeline.stream()
                .map(boundary -> boundary.getOperator())
                .map(op -> this.getSqlClause(op).split(", "))
                .flatMap(Arrays::stream)
                .distinct()
                .collect(Collectors.toList());

        // n^2
        // if the clause is already in e.g. a min() statement filter it out
        // TODO: this doesnt support nested select statements
        final String projectionStatement = nonDistinctSqlClauses.stream()
                /*
                 * .filter(str -> {
                 * for (final String str2 : distinctSqlClauses) {
                 * if (str2.contains(str) && str2 != str) {
                 * return false;
                 * }
                 * }
                 *
                 * return true;
                 * } )
                 */
                .collect(Collectors.joining(","));

        final String selectStatement = projectionStatement.length() == 0 ? "*" : projectionStatement;

        final String query = this.createSqlQuery(joinedTableNames, conditions, projection, joins,
                selectStatement);

        // get tasks who have sqlToStream connections:
        // this filter finds all operators who have
        // sqltostreamOperator
        final Stream<ExecutionTask> allBoundaryOperators = allTasksWithTableSources.stream()
                .filter(task -> task.getOutputChannel(0)
                        .getConsumers()
                        .stream()
                        .anyMatch(consumer -> consumer.getOperator() instanceof SqlToStreamOperator));

        final Collection<Instance> outBoundChannels = allBoundaryOperators
                .map(task -> this.instantiateOutboundChannel(task, optimizationContext))
                .collect(Collectors.toList()); //

        assert outBoundChannels.size() <= 1
                : "Only one boundary operator is allowed per execution stage, but found " + outBoundChannels.size();

        // set the string query generated above to each channel
        outBoundChannels.forEach(chann -> {
            chann.setSqlQuery(query);
            executionState.register(chann); // register at this execution stage so it gets executed
        });
    }

    /**
     * Searches depth-first through all projections, reduces connnected to a
     * boundary operator
     *
     * @param task  execution task at the boundary
     * @param stage current execution stage
     * @return an arraylist pipeline containing all projection & reductions from the
     *         boundary operator
     */
    private ArrayList<ExecutionTask> dfsProjectionPipeline(final ExecutionTask task, final ExecutionStage stage) {
        final ArrayList<ExecutionTask> pipeline = new ArrayList<>();
        ExecutionTask current = task;
        while (current.getOperator() instanceof JdbcGlobalReduceOperator
                || current.getOperator() instanceof JdbcProjectionOperator) {
            pipeline.add(current);
            // System.out.println("at current task: " + task);
            current = stage.getPreceedingTask(current).iterator().next(); // should only be one task in practice
        }

        return pipeline;
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

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                       whose outbound {@link SqlQueryChannel}
     *                                   should be instantiated
     * @param optimizationContext        provides information about the
     *                                   {@link ExecutionTask}
     * @param predecessorChannelInstance preceeding {@link SqlQueryChannel.Instance}
     *                                   to keep track of lineage
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
            final OptimizationContext optimizationContext,
            final SqlQueryChannel.Instance predecessorChannelInstance) {
        final SqlQueryChannel.Instance newInstance = this.instantiateOutboundChannel(task, optimizationContext);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }

    /**
     * Creates a SQL query.
     *
     * @param tableName  the table to be queried
     * @param conditions conditions for the {@code WHERE} clause
     * @param projection projection for the {@code SELECT} clause
     * @param joins      join clauses for multiple {@code JOIN} clauses
     * @return the SQL query
     */
    protected String createSqlQuery(final String tableName, final Collection<String> conditions,
            final String projection,
            final Collection<String> joins, final String selectStatement) {
        final StringBuilder sb = new StringBuilder(1000);

        final Set<String> projectionTableNames = Arrays.stream(projection.split(","))
                .map(name -> name.split("\\.")[0])
                .map(String::trim)
                .collect(Collectors.toSet());
        final Set<String> joinTableNames = joins.stream()
                .map(query -> query.split(" ON ")[0].replace("JOIN ", "").split(" AS ")[0])
                .collect(Collectors.toSet());
        final Set<String> joinTableAliases = joins.stream()
                .map(query -> query.split(" ON ")[0].replace("JOIN ", "").split(" AS ")[1])
                .collect(Collectors.toSet());
        final Set<String> leftJoinTableNames = joins.stream()
                .map(query -> query.split(" ON ")[1].split("=")[0].split("\\.")[0])
                .collect(Collectors.toSet());
        final Set<String> rightJoinTableNames = joins.stream()
                .map(query -> query.split(" ON ")[1].split("=")[1].split("\\.")[0])
                .collect(Collectors.toSet());

        // Union of projectionTableNames, leftJoinTableNames, and rightJoinTableNames
        final Set<String> unionSet = new HashSet<>();
        unionSet.addAll(projectionTableNames);
        unionSet.addAll(leftJoinTableNames);
        unionSet.addAll(rightJoinTableNames);

        // Remove tables that will be joined on, from the from clause
        unionSet.removeAll(joinTableNames);
        // Remove aliases from the from statement:
        unionSet.removeAll(joinTableAliases);
        System.out.println("aliases: " + joinTableAliases);
        System.out.println("projections: " + projectionTableNames);
        System.out.println("left joins: " + leftJoinTableNames);
        System.out.println("right joins: " + rightJoinTableNames);
        System.out.println("joins: " + joinTableNames);
        System.out.println(unionSet);

        // match JOIN x AS x* ON x*.col = y.col
        // group1: x
        // group2: x*
        // group3: x*.col
        // group4: y.col
        final String regex = "JOIN\\s+(?<joiningTable>\\w+)(?:\\s+AS\\s+(?<alias>\\w+))?\\s+ON\\s+(?<left>\\w+\\.\\w+)\\s*=\\s*(?<right>\\w+\\.\\w+)";
        final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        final Matcher matcher = pattern.matcher(joins.stream().findFirst().orElse(""));

        matcher.find();

        final String nonUsedTable = matcher.group(1).equals(matcher.group(3).split("\\.")[0]) ? matcher.group(4) : matcher.group(3);
        unionSet.add(nonUsedTable.split("\\.")[0]);
        System.out.println("matcher 1: " + matcher.group(1));
        System.out.println("matcher 3: " + matcher.group(3));
        System.out.println("non used table: " + nonUsedTable);
        System.out.println("first join: " + joins.stream().findFirst().get());

        final String requiredFromTableNames = unionSet.stream().collect(Collectors.joining(", "));

        sb.append("SELECT ").append(selectStatement).append(" FROM ").append(requiredFromTableNames);

        if (!joins.isEmpty()) {
            final String separator = " ";
            for (final String join : joins) {
                sb.append(separator).append(join);
            }
        }
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (final String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }
        sb.append(';');
        System.out.println("Decompiled into sql query: " + sb.toString());
        return sb.toString();
    }

    /**
     * Creates a SQL clause that corresponds to the given {@link Operator}.
     *
     * @param operator for that the SQL clause should be generated
     * @return the SQL clause
     */
    private String getSqlClause(final Operator operator) {
        return ((JdbcExecutionOperator) operator).createSqlClause(this.connection, this.functionCompiler);
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

    private void saveResult(final FileChannel.Instance outputFileChannelInstance, final ResultSet rs)
            throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(
                outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                // System.out.println(rs.getInt(1) + " " + rs.getString(2));
                final ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
