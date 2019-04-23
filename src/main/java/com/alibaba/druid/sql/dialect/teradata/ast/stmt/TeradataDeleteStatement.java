package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataOutputVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

public class TeradataDeleteStatement extends SQLDeleteStatement {

    private SQLTableSource from;
    private SQLTableSource using;
    private SQLOrderBy orderBy;
    private SQLIdentifierExpr all;

    public TeradataDeleteStatement() {
        super(JdbcConstants.TERADATA);
    }

    public SQLTableSource getFrom() {
        return from;
    }

    public SQLTableSource getUsing() {
        return using;
    }

    public void setUsing(SQLTableSource using) {
        this.using = using;
    }

    public void setFrom(SQLTableSource from) {
        this.from = from;
    }

    public SQLOrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(SQLOrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public SQLIdentifierExpr getAll() {
        return all;
    }

    public void setAll(SQLIdentifierExpr all) {
        this.all = all;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof TeradataASTVisitor) {
            accept0((TeradataASTVisitor) visitor);
        } else {
            throw new IllegalArgumentException("not support visitor type : " + visitor.getClass().getName());
        }
    }

    public void output(StringBuffer buf) {
        new TeradataOutputVisitor(buf).visit(this);
    }

    protected void accept0(TeradataASTVisitor visitor) {

        if (visitor.visit(this)) {
            acceptChild(visitor, getTableSource());
            acceptChild(visitor, getWhere());
            acceptChild(visitor, getFrom());
            acceptChild(visitor, getUsing());
            acceptChild(visitor, orderBy);
            acceptChild(visitor, all);
        }

        visitor.endVisit(this);
    }

}
