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

package org.apache.wayang.api.sql.calcite.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

import org.apache.wayang.core.api.Configuration;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ModelParser {
    private final Configuration configuration;
    private final JsonNode json;

    /**
     * This method allows you to specify the Calcite path, useful for testing.
     * See also {@link #ModelParser(Configuration)} and {@link #ModelParser()}.
     *
     * @param configuration    An empty configuration. Usage:
     *                         {@code Configuration configuration = new ModelParser(new Configuration(), calciteModelPath).setProperties();}
     * @param calciteModelPath Path to the JSON object containing the Calcite
     *                         model/schema.
     * @throws IOException    If an I/O error occurs.
     * @throws ParseException If unable to parse the file at
     *                        {@code calciteModelPath}.
     */
    public ModelParser(final Configuration configuration, final String calciteModelPath)
            throws IOException, ParseException {
        this.configuration = configuration;
        System.out.println("reading path: " + new File(calciteModelPath).toPath());
        final String calciteModel = Files.readString(new File(calciteModelPath).toPath());
        final ObjectMapper objectMapper = new ObjectMapper();

        this.json = objectMapper.readTree(calciteModel);
    }

    public ModelParser() throws IOException, ParseException {
        System.out.println("reading default");
        System.out.println("got file: " + new File("wayang-api/wayang-api-sql/src/main/resources/model.json").toPath());
        final String jsonString = Files
                .readString(new File("wayang-api/wayang-api-sql/src/main/resources/model.json").toPath());
        final ObjectMapper objectMapper = new ObjectMapper();

        this.json = objectMapper.readTree(jsonString);
        this.configuration = null;
    }

    public ModelParser(final Configuration configuration) throws IOException, ParseException {
        final String calciteModel = "{\"calcite\":" + configuration.getStringProperty("wayang.calcite.model")
                + ",\"separator\":\";\"}";
        final ObjectMapper objectMapper = new ObjectMapper();
        this.json = objectMapper.readTree(calciteModel);

        this.configuration = configuration;
    }

    /**
     * This method allows you to specify the Calcite path, useful for testing.
     * See also {@link #ModelParser(Configuration)} and {@link #ModelParser()}.
     *
     * @param configuration An empty configuration. Usage:
     *                      {@code Configuration configuration = new ModelParser(new Configuration(), calciteModelPath).setProperties();}
     * @param calciteModel  JSONized object of your calcite model
     * @throws IOException    If an I/O error occurs.
     * @throws ParseException If unable to parse the file at
     *                        {@code calciteModelPath}.
     */
    public ModelParser(final Configuration configuration, final JsonNode calciteModel)
            throws IOException, ParseException {
        this.configuration = configuration;
        this.json = calciteModel;
    }

    public Configuration setProperties() {
        final JsonNode calciteObj = json.get("calcite");
        final String calciteModel = calciteObj.toString();
        configuration.setProperty("wayang.calcite.model", calciteModel);

        final JsonNode schemas = calciteObj.get("schemas");

        final Iterator<JsonNode> itr = schemas.iterator();

        while (itr.hasNext()) {
            final JsonNode next = itr.next();
            if (next.get("name").equals("postgres")) {
                final JsonNode operand = next.get("operand");
                configuration.setProperty("wayang.postgres.jdbc.url", operand.get("jdbcUrl").toString());
                configuration.setProperty("wayang.postgres.jdbc.user", operand.get("jdbcUser").toString());
                configuration.setProperty("wayang.postgres.jdbc.password", operand.get("jdbcPassword").toString());
            }
        }
        return configuration;
    }

     public String getFsPath() {
        final JsonNode calciteObj = json.get("calcite");
        final JsonNode schemas = calciteObj.get("schemas");

        final Iterator<JsonNode> schemaIterator = schemas.iterator();

        while (schemaIterator.hasNext()) {
            final JsonNode next = schemaIterator.next();
            if (next.get("name").asText().equals("fs")) {
                final JsonNode operand = next.get("operand");

                return operand.get("directory").asText();
            }
        }
        return null;
    }

    public String getSeparator() {
        return (String) json.get("separator").asText();
    }
}
