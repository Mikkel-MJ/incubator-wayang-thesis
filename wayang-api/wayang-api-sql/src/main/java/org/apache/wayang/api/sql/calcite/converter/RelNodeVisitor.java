package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;

import com.google.common.collect.ImmutableList;

public class RelNodeVisitor extends RelShuttleImpl {

    @Override
    public RelNode visit(TableScan node) {
        UUID randomHint = UUID.randomUUID();
        List<RelHint> newHints = ImmutableList.of(RelHint.builder("uuid")
                .hintOption(randomHint.toString())
                .build());

            return LogicalTableScan.create(
                node.getCluster(),
                node.getTable(),
                newHints
            );
    }

}
