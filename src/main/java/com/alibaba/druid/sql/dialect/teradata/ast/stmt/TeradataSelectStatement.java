package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.Collections;
import java.util.List;

public class TeradataSelectStatement extends TeradataStatementImpl {

    protected TeradataSelect select;

    public TeradataSelectStatement() {
        super();
    }

    public TeradataSelectStatement(TeradataSelect select) {
        this.setSelect(select);
    }

    public TeradataSelectStatement(TeradataSelect select, String dbType) {
        this(select);
    }

    public TeradataSelect getSelect() {
        return this.select;
    }

    public void setSelect(TeradataSelect select) {
        if (select != null) {
            select.setParent(this);
        }
        this.select = select;
    }

    public void output(StringBuffer buf) {
        this.select.output(buf);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof TeradataASTVisitor) {
            accept0((TeradataASTVisitor) visitor);
        }
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.select);
        }
        visitor.endVisit(this);
    }

    public SQLSelectStatement clone() {
        SQLSelectStatement x = new SQLSelectStatement();
        if (select != null) {
            x.setSelect(select.clone());
        }
        if (headHints != null) {
            for (SQLCommentHint h : headHints) {
                SQLCommentHint h2 = h.clone();
                h2.setParent(x);
            }
        }
        return x;
    }

    @Override
    public List<SQLObject> getChildren() {
        return Collections.<SQLObject>singletonList(select);
    }

    public boolean addWhere(SQLExpr where) {
        return select.addWhere(where);
    }

}
