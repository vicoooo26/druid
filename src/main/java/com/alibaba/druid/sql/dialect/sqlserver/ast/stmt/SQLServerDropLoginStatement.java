package com.alibaba.druid.sql.dialect.sqlserver.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerStatementImpl;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLServerDropLoginStatement extends SQLServerStatementImpl implements SQLDropStatement {

    private List<SQLExpr> logins = new ArrayList<SQLExpr>(2);

    public SQLServerDropLoginStatement() {

    }


    public List<SQLExpr> getLogin() {
        return logins;
    }

    public void addLogin(SQLExpr login) {
        if (login != null) {
            login.setParent(this);
        }
        this.logins.add(login);
    }


    @Override
    public void accept0(SQLServerASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, logins);
        }
        visitor.endVisit(this);
    }

    @Override
    public List getChildren() {
        return logins;
    }
}
