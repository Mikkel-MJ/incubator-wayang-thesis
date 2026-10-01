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
package org.apache.wayang.ml.validation;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.ml.encoding.TreeNode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

/**
 * Class used for specifying validation rules on given platform choices
 */
public abstract class ValidationRule {

    public static final String VALIDATIONS_FILE_KEY = "wayang.ml.validations.file";

    /** Shared lock so concurrent rules don't interleave lines in the same file. */
    private static final Object LOG_LOCK = new Object();

    protected final Configuration configuration;

    protected ValidationRule(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void validate(
        Float[][] choices,
        long[][][] indexes,
        TreeNode tree
    ) {}

    /**
     * Records that this rule was actually applied by appending the concrete
     * class name to the file configured under {@value #VALIDATIONS_FILE_KEY}.
     * Does nothing if the property is not set.
     */
    protected void logApplication(String message) {
        if (this.configuration == null) {
            return;
        }

        Optional<String> logFile =
            this.configuration.getOptionalStringProperty(VALIDATIONS_FILE_KEY);

        if (!logFile.isPresent()) {
            return;
        }

        synchronized (LOG_LOCK) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile.get(), true))) {
                writer.write("[" + this.getClass().getName() + "]: " + message);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
