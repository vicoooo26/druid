package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateDatabaseStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface TeradataASTVisitor extends SQLASTVisitor {
    boolean visit(TeradataCreateDatabaseStatement x);

    void endVisit(TeradataCreateDatabaseStatement x);


}
