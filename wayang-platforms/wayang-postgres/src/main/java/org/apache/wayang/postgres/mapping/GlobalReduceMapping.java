package org.apache.wayang.postgres.mapping;

import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.JVMRecord;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaGlobalReduceOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.postgres.operators.PostgresGlobalReduceOperator;
import org.apache.wayang.postgres.platform.PostgresPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalReduceOperator} to {@link PostgresGlobalReduceOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalReduceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<GlobalReduceOperator<Record>> operatorPattern = new OperatorPattern<>(
                "reduce", new GlobalReduceOperator<Record>(null, DataSetType.createDefault(Record.class)), false)
                .withAdditionalTest(op -> {
                    return Mappings.isValidPostgres(op);
                });
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalReduceOperator>(
                (matchedOperator, epoch) -> new PostgresGlobalReduceOperator<>(matchedOperator).at(epoch)
        );
    }
}
