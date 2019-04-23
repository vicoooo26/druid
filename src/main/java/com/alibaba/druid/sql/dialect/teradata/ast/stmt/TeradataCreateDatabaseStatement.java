package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

public class TeradataCreateDatabaseStatement extends TeradataStatementImpl implements SQLCreateStatement {
    private SQLName name;
    private SQLExpr perm;
    private SQLExpr spool;

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public SQLExpr getPerm() {
        return perm;
    }

    public void setPerm(SQLExpr perm) {
        this.perm = perm;
    }

    public SQLExpr getSpool() {
        return spool;
    }

    public void setSpool(SQLExpr spool) {
        this.spool = spool;
    }

    @Override
    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, perm);
            acceptChild(visitor, spool);
        }
        visitor.endVisit(this);
    }
}

