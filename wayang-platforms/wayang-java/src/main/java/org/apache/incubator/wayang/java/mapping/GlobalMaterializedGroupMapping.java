package org.apache.incubator.wayang.java.mapping;

import org.apache.incubator.wayang.basic.operators.GlobalMaterializedGroupOperator;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.java.operators.JavaGlobalMaterializedGroupOperator;
import org.apache.incubator.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalMaterializedGroupOperator} to {@link JavaGlobalMaterializedGroupOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalMaterializedGroupMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "group", new GlobalMaterializedGroupOperator<>(DataSetType.none(), DataSetType.groupedNone()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalMaterializedGroupOperator>(
                (matchedOperator, epoch) -> new JavaGlobalMaterializedGroupOperator<>(matchedOperator).at(epoch)
        );
    }
}