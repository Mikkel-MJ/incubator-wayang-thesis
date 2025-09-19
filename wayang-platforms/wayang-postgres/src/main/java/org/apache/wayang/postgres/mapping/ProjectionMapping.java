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
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.postgres.operators.PostgresProjectionOperator;
import org.apache.wayang.postgres.platform.PostgresPlatform;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.Slot;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * /**
 * Mapping from {@link MapOperator} to {@link PostgresProjectionOperator}.
 */
public class ProjectionMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<MapOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "projection",
                new MapOperator<>(
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class)
                        ),
                false
        )
        .withAdditionalTest(op -> op.getFunctionDescriptor() instanceof ProjectionDescriptor)
        .withAdditionalTest(op -> op.getNumInputs() == 1) // No broadcasts.
        .withAdditionalTest(op -> {
            return !isAddAggColsMapping(op);
        })
        .withAdditionalTest(op -> {
            return Mappings.isValidPostgres(op);
        });

        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator<Record, Record>>(
                (matchedOperator, epoch) -> {
                        return new PostgresProjectionOperator(matchedOperator).at(epoch);
                }
        );
    }

    /* I am sorry for this test, but it test if this Map is the predecessor of a GlobalReduce.
     * If it is, this map can't be in postgres, as all GlobalReduce's from the sql api hold AddAggCols
     * which won't be executed if its in postgres. AddAggCols needs to be executed for effective counting though.
     */
    private static boolean isAddAggColsMapping(Operator op) {
        List<Slot<?>> slots = op.getOutermostOutputSlots(op.getOutput(0))
                    .stream()
                    .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                    .collect(Collectors.toList());
        for (Slot<?> slot : slots) {
            Operator owner = slot.getOwner();

            if (owner instanceof OperatorAlternative) {
                List<Operator> alternatives = ((OperatorAlternative) owner).getAlternatives()
                    .stream()
                    .map(parent -> parent.getContainedOperators().stream().findFirst().get())
                    .collect(Collectors.toList());

                for (Operator o : alternatives) {
                        if(o instanceof GlobalReduceOperator){
                            return true;
                        }
                }
            } else {
                if(owner instanceof GlobalReduceOperator){
                    return true;
                }
            }
        }
        return false;
    }
}
