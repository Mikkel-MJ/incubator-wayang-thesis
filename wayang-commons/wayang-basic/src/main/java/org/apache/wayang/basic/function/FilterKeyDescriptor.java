package org.apache.wayang.basic.function;

import java.util.List;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;

public class FilterKeyDescriptor<I> extends FunctionDescriptor {
    private final SerializablePredicate<I> javaImplementation;

    private final List<String> fields;

    private final List<String> aliases;

    public FilterKeyDescriptor(final LoadProfileEstimator loadProfileEstimator,
            final SerializablePredicate<I> javaImplementation, final List<String> fields, final List<String> aliases) {
        super(loadProfileEstimator);
        this.javaImplementation = javaImplementation;
        this.fields = fields;
        this.aliases = aliases;
    }

    public SerializablePredicate<I> getJavaImplementation() {
        return javaImplementation;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<String> getAliases() {
        return aliases;
    }

}
