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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.JoinKeyDescriptor;
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
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.operators.SqlToFlinkDataSetOperator;
import org.apache.wayang.jdbc.operators.SqlToRddOperator;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

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

    class AliasMap extends HashMap<Operator, String> {
        private int counter = 0;

        public String computeIfAbsent(final Operator operator) {
            return super.computeIfAbsent(operator, op -> "subquery" + counter++);
        }
    }

    public static String visitTask(final ExecutionTask task, final ExecutionStage stage, final int subqueryCount,
            final AliasMap aliasMap) {
        final Operator operator = task.getOperator();
        final List<ExecutionTask> nextTasks = stage.getPreceedingTask(task);

        System.out.println("[VisitTask.aliasMap]: " + aliasMap);
        System.out.println("[VisitTask.subqueryCount]: " + subqueryCount);

        if (operator instanceof JdbcJoinOperator) {
            assert nextTasks.size() == 2
                    : "amount of next next operators in join operator was not two, got: " + nextTasks.size();
            final JdbcJoinOperator<?> join = (JdbcJoinOperator<?>) operator;

            System.out.println("[VisitTask.Join.leftKD]: " + join.getKeyDescriptor0().getSqlImplementation());
            System.out.println("[VisitTask.Join.rightKD]: " + join.getKeyDescriptor1().getSqlImplementation());

            System.out.println("[VisitTask.Join.getClass]: " + join.getKeyDescriptor0().getClass());

            final JoinKeyDescriptor<?, ?> joinKeyDescriptor = (JoinKeyDescriptor<?, ?>) join.getKeyDescriptor0();
            final JoinKeyDescriptor<?, ?> joinKeyDescriptor2 = (JoinKeyDescriptor<?, ?>) join.getKeyDescriptor1();

            System.out.println("[VisitTask.JoinKey0]: " + joinKeyDescriptor.getKey());
            System.out.println("[VisitTask.JoinKey1]: " + joinKeyDescriptor2.getKey());

            // we recursively visit left and right nodes and treat them as subqueries
            final String left = visitTask(nextTasks.get(0), stage, subqueryCount + 1, aliasMap);
            final String right = visitTask(nextTasks.get(1), stage, subqueryCount + 1, aliasMap);

            // create alias from inner join left & right table
            final String alias = aliasMap.computeIfAbsent(operator);
            final String leftAlias = "left" + alias;
            final String rightAlias = "right" + alias;

            final boolean isLeftFirst = joinKeyDescriptor.getKey() < joinKeyDescriptor2.getKey();

            // the left join key descriptor contains projections from the left table in a
            // join
            // likewise with the right key descriptor. So we need to make sure that
            // leftAlias + rightAlias match this expectation
            final String[] projections = isLeftFirst
                    ? Stream.concat(
                            Arrays.stream(joinKeyDescriptor.getProjectedFields())
                                    .map(field -> leftAlias + "." + field),
                            Arrays.stream(joinKeyDescriptor2.getProjectedFields())
                                    .map(field -> rightAlias + "." + field))
                            .toArray(String[]::new)
                    : Stream.concat(
                            Arrays.stream(joinKeyDescriptor2.getProjectedFields())
                                    .map(field -> rightAlias + "." + field),
                            Arrays.stream(joinKeyDescriptor.getProjectedFields())
                                    .map(field -> leftAlias + "." + field))
                            .toArray(String[]::new);

            final String[] aliases = Stream.concat(
                    Arrays.stream(joinKeyDescriptor.getAliases()),
                    Arrays.stream(joinKeyDescriptor2.getAliases()))
                    .toArray(String[]::new);

            assert projections.length == aliases.length : "Amount of projections did not match the amount of aliases.";

            final String[] aliasedProjections = new String[projections.length];

            for (int i = 0; i < aliasedProjections.length; i++) {
                aliasedProjections[i] = projections[i] + " AS " + aliases[i];
            }

            final String selectStatement = Arrays.stream(aliasedProjections).collect(Collectors.joining(","));

            // setup join filter condition
            final String leftField = join.getKeyDescriptor0().getSqlImplementation();
            final String rightField = join.getKeyDescriptor1().getSqlImplementation();

            assert leftField != null : "Left join field in filter was null.";
            assert rightField != null : "Right join field in filter was null.";

            final String filter = String.format("%s.%s = %s.%s",
                    !isLeftFirst ? leftAlias : rightAlias, leftField,
                    !isLeftFirst ? rightAlias : leftAlias, rightField);

            System.out.println("[visitTask.Join.leftOp]: " + nextTasks.get(0));
            System.out.println("[visitTask.Join.rightOp]: " + nextTasks.get(1));
            System.out.println("[visitTask.Join.left]: " + left);
            System.out.println("[visitTask.Join.right]: " + right);

            System.out.println("[VisitTask.visitJoin.isLeftFirst]: " + isLeftFirst);
            System.out.println("[VisitTask.visitJoin.projections]: " + List.of(projections));
            System.out.println("[VisitTask.visitJoin.aliases]: " + List.of(aliases));
            System.out.println("[VisitTask.visitJoin.aliasedProj]: " + List.of(aliasedProjections));

            return "SELECT " + selectStatement + " FROM (" + left + ") AS left" + alias + " INNER JOIN (" + right
                    + ") AS right"
                    + alias + "  ON " + filter;
        } else if (operator instanceof JdbcProjectionOperator) {
            assert nextTasks.size() == 1
                    : "amount of next operators of projection operator was not one, got: " + nextTasks.size();
            final JdbcProjectionOperator<?, ?> projection = (JdbcProjectionOperator<?, ?>) operator;

            final String columns = projection.getFunctionDescriptor().getSqlImplementation();

            System.out.println("[VisitTask.Projection.columns]: " + columns);

            final String input = visitTask(nextTasks.get(0), stage, subqueryCount + 1, aliasMap);

            // handle aliases
            final String alias = aliasMap.computeIfAbsent(operator);

            System.out.println("[VisitTask.Projection.input]: " + input);
            final String returnStmnt = columns == null
                    ? input
                    : "SELECT " + columns + " FROM (" + input + ") AS " + alias;

            System.out.println("[VisitTask.Projection.return]: " + returnStmnt);
            return returnStmnt;
        } else if (operator instanceof JdbcFilterOperator) {
            assert nextTasks.size() == 1
                    : "amount of next operators of filter operator was not one, got: " + nextTasks.size();
            final JdbcFilterOperator filter = (JdbcFilterOperator) operator;

            final String input = visitTask(nextTasks.get(0), stage, subqueryCount + 1, aliasMap);
            final String alias = aliasMap.computeIfAbsent(operator);

            System.out.println("[VisitTask.Filter]: " + filter.getPredicateDescriptor().getSqlImplementation());

            return "SELECT * FROM (" + input + ") AS " + alias + " WHERE "
                    + filter.getPredicateDescriptor().getSqlImplementation();
        } else if (operator instanceof JdbcGlobalReduceOperator) {
            assert nextTasks.size() == 1
                    : "amount of next operators of reduce operator was not one, got: " + nextTasks.size();
            final JdbcGlobalReduceOperator<?> reduceOperator = (JdbcGlobalReduceOperator<?>) operator;

            final String input = visitTask(nextTasks.get(0), stage, subqueryCount + 1, aliasMap);
            final String reduce = reduceOperator.getReduceDescriptor().getSqlImplementation();
            final String alias = aliasMap.computeIfAbsent(operator);

            System.out.println("[VisitTask.Reduce]: " + reduce);

            return "SELECT " + reduce + " FROM (" + input + ") AS " + alias;
        } else if (operator instanceof JdbcTableSource) {
            assert nextTasks.size() == 0
                    : "amount of next operators of reduce operator was not zero, got: " + nextTasks.size();
            final JdbcTableSource table = (JdbcTableSource) operator;
            final String alias = aliasMap.computeIfAbsent(operator);

            System.out.println("[VisitTask.TableSource.getTableName()]: " + table.getTableName());

            return "SELECT * FROM " + table.getTableName() + " AS " + alias;
        } else {
            throw new UnsupportedOperationException("Operator not supported in JDBC executor: " + operator);
        }
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
            // arg0 69a {12a} arg1 A12{}
            if (ordering.get(arg0).contains(arg1)) {
                return -1;
            }
            if (ordering.get(arg1).contains(arg0)) {
                return 1;
            }
            return 0;
        }
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext,
            final ExecutionState executionState) {
        final String query = stage.getTerminalTasks().stream().map(task -> visitTask(task, stage, 0, new AliasMap()))
                .findAny().orElseThrow(() -> new WayangException("Could not produce Sql in JdbcExecutor."));

        System.out.println("[JdbcExecutor.query]: produced query: " + query);
        // order all tasks by whether or not a given task is reachable from another
        final List<ExecutionTask> allTasksWithTableSources = Arrays
                .stream(stage.getAllTasks().toArray(ExecutionTask[]::new))
                .sorted(new ExecutionTaskOrderingComparator(stage.canReachMap()))
                .collect(Collectors.toList());

        // get tasks who have sqlToStream connections:
        // this filter finds all operators who have
        // sqltostreamOperator
        final Stream<ExecutionTask> allBoundaryOperators = allTasksWithTableSources.stream()
                .filter(task -> task.getOutputChannel(0)
                        .getConsumers()
                        .stream()
                        // TODO: change this to fit every conversion of Postgres at least
                        .anyMatch(consumer -> consumer.getOperator() instanceof SqlToStreamOperator
                                || consumer.getOperator() instanceof SqlToFlinkDataSetOperator
                                || consumer.getOperator() instanceof SqlToRddOperator));

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

        /*
         * unionSet.addAll(leftJoinTableNames);
         * unionSet.addAll(rightJoinTableNames);
         */

        // Remove tables that will be joined on, from the from clause
        unionSet.removeAll(joinTableNames);
        // Remove aliases from the from statement:
        unionSet.removeAll(joinTableAliases);

        // match JOIN x AS x* ON x*.col = y.col
        // group1: x
        // group2: x*
        // group3: x*.col
        // group4: y.col
        if (joins.size() > 0) {
            final String regex = "JOIN\\s+(?<joiningTable>\\w+)(?:\\s+AS\\s+(?<alias>\\w+))?\\s+ON\\s+(?<left>\\w+\\.\\w+)\\s*=\\s*(?<right>\\w+\\.\\w+)";
            final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

            final String joinClause = joins.stream().findFirst().orElse("");
            final Matcher matcher = pattern.matcher(joinClause);

            if (matcher.find()) {
                final String joiningTable = matcher.group("joiningTable");
                final String left = matcher.group("left");
                final String right = matcher.group("right");

                if (joiningTable != null && left != null && right != null) {
                    final String leftTable = left.split("\\.")[0];
                    final String rightTable = right.split("\\.")[0];

                    final String nonUsedTable = joiningTable.equals(leftTable) ? right : left;
                    unionSet.add(nonUsedTable.split("\\.")[0]);
                }
            }
        }

        final String requiredFromTableNames = unionSet.stream().collect(Collectors.joining(", "));

        sb.append("SELECT ").append(selectStatement).append(" FROM ").append(requiredFromTableNames);
        // build joins in sql query
        if (!joins.isEmpty()) {
            final String separator = " ";
            for (final String join : joins) {
                sb.append(separator).append(join);
            }
        }

        // build filters
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (final String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }
        sb.append(';');

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
