package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.wayang.api.sql.calcite.converter.CalciteSerialization.CalciteAggSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AggregateFunction extends CalciteAggSerializable implements FunctionDescriptor.SerializableBinaryOperator<Record>{
    public AggregateFunction(final List<AggregateCall> aggregateCalls) {
        super(aggregateCalls.toArray(AggregateCall[]::new));
    }

    @Override
    public Record apply(final Record record1, final Record record2) {
        List<AggregateCall> aggregateCalls = Arrays.asList(super.serializables);
        
        final int l = record1.size();
        final Object[] resValues = new Object[l];
        int i;
        final boolean countDone = false;
    
        for (i = 0; i < l - aggregateCalls.size() - 1; i++) {
            resValues[i] = record1.getField(i);
        }
    
        for (final AggregateCall aggregateCall : aggregateCalls) {
            final String name = aggregateCall.getAggregation().getName();
            final Object field1 = record1.getField(i);
            final Object field2 = record2.getField(i);
            
            switch (name) {
                case "SUM":
                    resValues[i] = this.castAndMap(field1, field2, null, Long::sum, Integer::sum, Double::sum);
                    break;
                case "MIN":
                    resValues[i] = this.castAndMap(field1, field2, SqlFunctions::least, SqlFunctions::least, SqlFunctions::least, SqlFunctions::least);
                    break;
                case "MAX":
                    resValues[i] = this.castAndMap(field1, field2, SqlFunctions::greatest, SqlFunctions::greatest, SqlFunctions::greatest, SqlFunctions::greatest);
                    break;
                case "COUNT":
                    resValues[i] = this.castAndMap(field1, field2, null, null, null, null);
                    break;
                case "AVG":
                    resValues[i] = this.castAndMap(field1, field2, null, null, null, null);
                    break;
                default:
                    throw new IllegalStateException("Unsupported operation: " + name);
            }
            i++;
        }
    
        return new Record(resValues);
    }

    /**
     * Handles casts for the record class for each interior type.
     * @param a field of first record
     * @param b field of second record
     * @param stringMap mapping if the field is a string or null
     * @param longMap mapping if the field is a long or null
     * @param integerMap mapping if the field is a integer or null
     * @param doubleMap mapping if the field is a double or null
     * @return the result of the mapping being applied
     */
    private Object castAndMap(final Object a, final Object b, 
    final BiFunction<String, String, String> stringMap, 
    final BiFunction<Long, Long, Long> longMap,
    final BiFunction<Integer, Integer, Integer> integerMap,
    final BiFunction<Double, Double, Double> doubleMap) {
        if ((a == null || b == null) || (a.getClass() == b.getClass())) { //support operations between null and any class
            final Optional<Object> aWrapped = Optional.ofNullable(a); //objects can be null in this if statement due to condition above
            final Optional<Object> bWrapped = Optional.ofNullable(b);

            switch (aWrapped.orElse(bWrapped.orElse("")).getClass().getSimpleName()) {//force .getClass() to be safe so we can pass null objects to .apply methods.
                case "String":
                    return stringMap.apply((String) a, (String) b);
                case "Long":
                    return longMap.apply((Long) a, (Long) b);
                case "Integer":
                    return integerMap.apply((Integer) a, (Integer) b);
                case "Double":
                    return doubleMap.apply((Double) a, (Double) b);
                default:
                    throw new IllegalStateException("Unsupported operation between: " + aWrapped.getClass().toString() + " and: " + bWrapped.getClass().toString());
            }
        }
        throw new IllegalStateException("Unsupported operation between: " + a.getClass().getSimpleName() + " and: " + b.getClass().getSimpleName());
    }
}