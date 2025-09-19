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

package org.apache.wayang.postgres.mapping;

import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.JVMRecord;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.postgres.operators.PostgresProjectionOperator;
import org.apache.wayang.postgres.platform.PostgresPlatform;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;

import java.util.Collection;
import java.util.Collections;

/**
 * This is a placeholder mapping that maps the {@link MapOperator} (flattening)
 * that appears after {@link JoinOperator}
 * from the sql api, see {@link WayangJoinVisitor}, this is only necessary in
 * cases where this operator needs to be run on Postgres
 * because the logic of the join flattening is contained within Postgres' join
 * operator, and not this map operator.
 */
@SuppressWarnings("unchecked")
public class FlattenMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<MapOperator<Tuple2, Record>> operatorPattern = new OperatorPattern<>(
                "flatten",
                new MapOperator<Tuple2, Record>(
                        null,
                        DataSetType.createDefault(Tuple2.class),
                        //DataSetType.createDefault(ReflectionUtils.specify(Tuple2.class)),
                        DataSetType.createDefault(Record.class)),
                false)
                .withAdditionalTest(op -> op.getFunctionDescriptor() instanceof ProjectionDescriptor)
                .withAdditionalTest(op -> op.getNumInputs() == 1) // No broadcasts.
                /*
                .withAdditionalTest(op -> {
                    if(((OperatorAlternative)op.getEffectiveOccupant(0).getOwner()).getAlternatives().get(0).getContainedOperators().stream().findFirst().get() instanceof JoinOperator) {
                        return false;
                    }

                    return true;
                })*/
                .withAdditionalTest(op -> {
                    return Mappings.isValidPostgres(op);
                });
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator<Tuple2, Record>>(
                (matchedOperator, epoch) -> new PostgresProjectionOperator<>(matchedOperator).at(epoch));
    }
}
