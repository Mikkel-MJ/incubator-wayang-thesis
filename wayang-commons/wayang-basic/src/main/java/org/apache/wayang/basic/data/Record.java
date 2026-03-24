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

package org.apache.wayang.basic.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.apache.wayang.core.util.Copyable;
import org.apache.wayang.core.util.ReflectionUtils;

/**
 * A Type that represents a record with a schema, might be replaced with
 * something standard like JPA entity.
 */
public class Record implements Serializable, Copyable<Record>, Comparable<Record> {

    private final Object[] values;

    public Record(final Object... values) {
        this.values = values;
    }

    @Override
    public Record copy() {
        return new Record(this.values.clone());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;
        final Record record2 = (Record) o;
        return Arrays.equals(this.values, record2.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(values));
    }

    @Override
    public String toString() {
        return "Record" + Arrays.toString(this.values);
    }

    public Object getField(final int index) {
        if (index >= this.size()) {
            System.out.println(this);
        }

        return this.values[index];
    }

    /**
     * Retrieve a field as a {@code double}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code double} representation of the field
     */
    public double getDouble(final int index) {
        final Object field = this.values[index];
        return ReflectionUtils.toDouble(field);
    }

    /**
     * Retrieve a field as a {@code long}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code long} representation of the field
     */
    public long getLong(final int index) {
        final Object field = this.values[index];
        if (field instanceof Integer)
            return (Integer) field;
        else if (field instanceof Long)
            return (Long) field;
        else if (field instanceof Short)
            return (Short) field;
        else if (field instanceof Byte)
            return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as long.", field));
    }

    /**
     * Retrieve a field as a {@code int}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code int} representation of the field
     */
    public int getInt(final int index) {
        final Object field = this.values[index];
        if (field instanceof Integer)
            return (Integer) field;
        else if (field instanceof Short)
            return (Short) field;
        else if (field instanceof Byte)
            return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as int.", field));
    }

    /**
     * Retrieve a field as a {@link String}.
     *
     * @param index the index of the field
     * @return the field as a {@link String} (obtained via
     *         {@link Object#toString()}) or {@code null} if the field is
     *         {@code null}
     */
    public String getString(final int index) {
        final Object field = this.values[index];
        return field == null ? null : field.toString();
    }

    /**
     * Retrieve the size of this instance.
     *
     * @return the number of fields in this instance
     */
    public int size() {
        return this.values.length;
    }

    public Object[] getFields() {
        return this.values;
    }

    /**
     * Compares the fields of this record to the fields of another record.
     *
     * @param that another record not null
     * @return
     * @throws IllegalStateException if the two records do not have the same types
     *                               in {@link #values}
     */
    @Override
    public int compareTo(final Record that) throws IllegalStateException {
        for (int i = 0; i < this.values.length; i++) {
            if (!this.values[i].getClass().equals(that.values[i].getClass())) {
                throw new IllegalStateException(
                        "Tried to compare records with dissimilar classes. " +
                                "this item class: " + this.values[i].getClass() +
                                ", that item class: " + that.values[i].getClass());
            }

            if (!(this.values[i] instanceof Comparable)) {
                throw new IllegalStateException(
                        "Record value at index " + i + " is not Comparable: " +
                                this.values[i].getClass());
            }

            @SuppressWarnings("unchecked")
            final int cmp = ((Comparable<Object>) this.values[i]).compareTo(that.values[i]);

            if (cmp != 0)
                return cmp;
        }
        return Integer.compare(this.values.length, that.values.length);
    }
}
