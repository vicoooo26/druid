package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateDatabaseStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;

public class TeradataSchemaStatVisitor extends SchemaStatVisitor implements TeradataASTVisitor {
    @Override
    public boolean visit(TeradataCreateDatabaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataCreateDatabaseStatement x) {

    }
}
