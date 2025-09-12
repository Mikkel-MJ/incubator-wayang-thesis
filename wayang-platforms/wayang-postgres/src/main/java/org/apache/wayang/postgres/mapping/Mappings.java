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
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.postgres.operators.PostgresExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.Operator;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
/**
* Register for the {@link Mapping}s supported for this platform.
*/
public class Mappings {
    public static final Collection<Mapping> ALL = Arrays.asList(
            new FilterMapping(),
            new JoinMapping(),
            new ProjectionMapping(),
            new FlattenMapping(),
            new GlobalReduceMapping()
    );

    public static boolean isValidPostgres(Operator operator) {
        if (operator instanceof ExecutionOperator) {
            return operator instanceof PostgresExecutionOperator;
        }

        if (operator.getNumInputs() == 0) {
            if (operator instanceof OperatorAlternative) {
                List<OperatorAlternative.Alternative> alternatives = ((OperatorAlternative) operator).getAlternatives();

                long noOfPostgresAlts = alternatives
                    .stream()
                    .filter(op -> op.getContainedOperators().stream().findFirst().get() instanceof PostgresExecutionOperator)
                    .count();

                if (noOfPostgresAlts == 0) {
                    return false;
                }
            }
        }

        for (int i = 0; i < operator.getNumInputs(); i++) {
            Operator input = operator.getEffectiveOccupant(i).getOwner();

            if (!isValidPostgres(input)) {
                return false;
            }
        }

        return true;
    }
}
