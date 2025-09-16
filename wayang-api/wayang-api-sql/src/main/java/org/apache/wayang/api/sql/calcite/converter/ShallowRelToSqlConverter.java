package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;

public class ShallowRelToSqlConverter extends RelToSqlConverter {
    public ShallowRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(Project project) {
      // build SELECT clause from project.getProjects()
       final RexSqlStandardConvertletTable convertletTable =
        new RexSqlStandardConvertletTable();
       final RexToSqlNodeConverter rexToSqlNodeConverter =
        new RexToSqlNodeConverterImpl(convertletTable);
      List<SqlNode> selectList = new ArrayList<>();
      for (RexNode expr : project.getProjects()) {
        selectList.add(rexToSqlNodeConverter.convertNode(expr));
      }


      SqlNode sqlSelect = new SqlSelect(
          SqlParserPos.ZERO,
          SqlNodeList.EMPTY,
          new SqlNodeList(selectList, SqlParserPos.ZERO),
          null, // no FROM, since we’re not including inputs
          null, null, null, null, null, null, null, null, null);

      return result(sqlSelect, ImmutableList.of(Clause.SELECT), project, null);
    }
}

