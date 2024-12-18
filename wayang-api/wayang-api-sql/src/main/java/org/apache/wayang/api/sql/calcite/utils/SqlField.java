package org.apache.wayang.api.sql.calcite.utils;

import java.io.Serializable;

public class SqlField<T extends Serializable & Comparable<T>> implements Serializable, Comparable<T> {
    final T field;
    final Class<?> clazz;

    /**
     * A union type meant to represent the return types from query sql.
     * @param field Any object but in practice, String, Integer, Long ...
     */
    public SqlField(T field) {
        //
        this.field = field;
        this.clazz = field.getClass();
    }

    @Override
    public int compareTo(T arg0) {
        return field.compareTo(arg0);
    }
}
