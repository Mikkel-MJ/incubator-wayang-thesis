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

package org.apache.wayang.basic.function;

import java.util.List;
import java.util.function.Function;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.TransformationDescriptor;

public class JoinKeyDescriptor extends TransformationDescriptor<Record, Record> {
    private static class RecordImplementation implements SerializableFunction<Record, Record> {
        final List<Integer> keys;

        RecordImplementation(final List<Integer> keys) {
            this.keys = keys;
        }

        @Override
        public Record apply(final Record rec) {
            return new Record(keys.stream().map(rec::getField).toArray());
        }

    }

    private final List<String> aliases;

    private final List<String> fieldNames;

    private final Function<List<String>, String> createSqlFunc;

    /**
     * Descriptor for the extractor that gets the column from the left or right input of a join.
     * @param keys 
     * @param aliases
     * @param fieldNames
     */
    public JoinKeyDescriptor(final List<Integer> keys, final List<String> aliases, final List<String> fieldNames) {
        this(new RecordImplementation(keys), aliases, fieldNames);
    }

    private JoinKeyDescriptor(final RecordImplementation impl, final List<String> aliases,
            final List<String> fieldNames) {
        super(impl, Record.class, Record.class);
        this.aliases = aliases;
        this.fieldNames = fieldNames;
        this.createSqlFunc = null;
    }

    public JoinKeyDescriptor(final SerializableFunction<Record, Record> impl, final SerializableFunction<List<String>, String> createSqlFunction) {
        super(impl, Record.class, Record.class);
        this.aliases = null;
        this.fieldNames = null;
        this.createSqlFunc = createSqlFunction;
    }

    
    public JoinKeyDescriptor(final SerializableFunction<Record, Record> impl, final List<String> aliases, final List<String> fieldNames, final SerializableFunction<List<String>, String> createSqlFunction) {
        super(impl, Record.class, Record.class);
        this.aliases = aliases;
        this.fieldNames = fieldNames;
        this.createSqlFunc = createSqlFunction;
    }

    public boolean hasSqlImpl (){
        return createSqlFunc != null;        
    }

    public String createSqlString(final List<String> fieldNames){
        return this.createSqlFunc.apply(fieldNames);
    }

    public List<String> getAliases() {
        return aliases;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Integer> getkeys() {
        return ((RecordImplementation) this.getJavaImplementation()).keys;
    }
}