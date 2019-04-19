package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateDatabaseStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

public class TeradataOutputVisitor extends SQLASTOutputVisitor implements TeradataASTVisitor {
    public TeradataOutputVisitor(Appendable appender) {
        super(appender);
    }

    @Override
    public boolean visit(TeradataCreateDatabaseStatement x) {
        print0(ucase ? "CREATE DATABASE " : "create database ");
        return false;
    }

    @Override
    public void endVisit(TeradataCreateDatabaseStatement x) {

    }
}
