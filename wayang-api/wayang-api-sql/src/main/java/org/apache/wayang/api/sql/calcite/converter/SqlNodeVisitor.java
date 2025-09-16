package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;


public class SqlNodeVisitor implements SqlVisitor<Void> {
    public List<SqlNode> nodeList;

    public SqlNodeVisitor(List<SqlNode> nodeList) {
        this.nodeList = nodeList;
    }

	@Override
	public Void visit(SqlLiteral arg0) {
        return null;
	}

	@Override
	public Void visit(SqlCall call) {
        this.nodeList.add(call);
        return call.getOperator().acceptCall(this, call);
	}

	@Override
	public Void visit(SqlNodeList arg0) {
        for (SqlNode node : arg0.getList()) {
            if (node != null) {
                this.nodeList.add(node);
                node.accept(this);
            }
        }

        return null;
	}

	@Override
	public Void visit(SqlIdentifier arg0) {
        return null;
	}

	@Override
	public Void visit(SqlDataTypeSpec arg0) {
        return null;
	}

	@Override
	public Void visit(SqlDynamicParam arg0) {
        return null;
	}

	@Override
	public Void visit(SqlIntervalQualifier arg0) {
        return null;
	}

    /** Asks a {@code SqlNode} to accept this visitor. */
    @Override
    public Void visitNode(SqlNode n) {
        this.nodeList.add(n);

        return null;
    }

}
