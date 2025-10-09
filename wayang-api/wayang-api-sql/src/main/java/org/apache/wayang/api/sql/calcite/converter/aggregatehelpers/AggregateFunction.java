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
package org.apache.wayang.api.sql.calcite.converter.aggregatehelpers;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AggregateFunction
        implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final List<SqlKind> aggregateKinds;

    public AggregateFunction(final List<AggregateCall> aggregateCalls) {
        this.aggregateKinds = aggregateCalls.stream()
                .map(call -> call.getAggregation().getKind())
                .collect(Collectors.toList());
    }

    @Override
    public Record apply(final Record record1, final Record record2) {
        final int l = record1.size();
        final Object[] resValues = new Object[l];
        boolean countDone = false;

        for (int i = 0; i < l - aggregateKinds.size() - 1; i++) {
            resValues[i] = record1.getField(i);
        }

        int counter = l - aggregateKinds.size() - 1;
        for (final SqlKind kind : aggregateKinds) {
            final Object field1 = record1.getField(counter);
            final Object field2 = record2.getField(counter);

            switch (kind) {
                case SUM:
                    resValues[counter] = this.castAndMap(field1, field2, null, Long::sum, Integer::sum, Double::sum);
                    break;
                case MIN:
                    resValues[counter] = this.castAndMap(field1, field2, SqlFunctions::least, SqlFunctions::least,
                            SqlFunctions::least, SqlFunctions::least);
                    break;
                case MAX:
                    resValues[counter] = this.castAndMap(field1, field2, SqlFunctions::greatest, SqlFunctions::greatest,
                            SqlFunctions::greatest, SqlFunctions::greatest);
                    break;
                case COUNT:
                    // since aggregates inject an extra column for counting before,
                    // see AggregateAddCols. the column we operate on are integer counts,
                    // which means we can eagerly get the fields as integers and simply sum
                    assert (field1 instanceof Integer && field2 instanceof Integer)
                            : "Expected to find integers for count but found: " + field1 + " and " + field2;
                    final Object count = Integer.class.cast(field1) + Integer.class.cast(field2);
                    resValues[counter] = count;
                    break;
                case AVG:
                    assert (field1 instanceof Integer && field2 instanceof Integer)
                            : "Expected to find integers for count but found: " + field1 + " and " + field2;
                    final Object avg = Integer.class.cast(field1) + Integer.class.cast(field2);

                    resValues[counter] = avg;

                    if (!countDone) {
                        resValues[l - 1] = record1.getInt(l - 1) + record2.getInt(l - 1);
                        countDone = true;
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported operation: " + kind);
            }
            counter++;
        }
        return new Record(resValues);
    }

    /**
     * Handles casts for the record class for each interior type.
     *
     * @param a          field of first record
     * @param b          field of second record
     * @param stringMap  mapping if the field is a string or null
     * @param longMap    mapping if the field is a long or null
     * @param integerMap mapping if the field is a integer or null
     * @param doubleMap  mapping if the field is a double or null
     * @return the result of the mapping being applied
     */
    private Object castAndMap(final Object a, final Object b,
            final BiFunction<String, String, Object> stringMap,
            final BiFunction<Long, Long, Object> longMap,
            final BiFunction<Integer, Integer, Object> integerMap,
            final BiFunction<Double, Double, Object> doubleMap) {
        // Lets fucking disallow nulls
        //
        //
        if (a == null && b == null) {
            return null;
        }

        if (a == null) {
            return b;
        }

        if (b == null) {
            return a;
        }

        // support operations between null and any
        // class
        // objects can be null in this if statement due to
        // condition above
        switch (a.getClass().getSimpleName()) {
            case "String":
                return stringMap.apply((String) a, (String) b);
            case "Long":
                return longMap.apply((Long) a, (Long) b);
            case "Integer":
                return integerMap.apply((Integer) a, (Integer) b);
            case "Double":
                return doubleMap.apply((Double) a, (Double) b);
            default:
                throw new IllegalStateException("Unsupported operation between: " + a.getClass().toString()
                        + " and: " + b.getClass().toString());
        }
    }
}
