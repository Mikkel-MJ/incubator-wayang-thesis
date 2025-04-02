package org.apache.wayang.api.sql.calcite.converter.sorthelpers;

import java.util.List;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexNode;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;


public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Record> {
    final List<Direction> directions;
    final List<Integer> collationIndexes;
    final RexNode fetch;
    final int offset;

    public KeyExtractor(List<Direction> collationDirections, List<Integer> collationIndexes, RexNode fetch, int offset){
        if (fetch != null) throw new UnsupportedOperationException("Sorts with fetch relational expressions like: " + fetch + " are not yet supported.");
        this.directions = collationDirections;
        this.collationIndexes = collationIndexes;
        this.fetch = fetch;
        this.offset = offset;
    }

    //TODO: we need to be able to specifiy a sort direction, limit i.e. how many rows we get, and more
    @Override
    public Record apply(Record record) {
        return new Record(collationIndexes.stream().map(record::getField).toArray());
    }
}
