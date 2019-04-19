package com.alibaba.druid.sql.dialect.sqlserver.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerStatementImpl;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerASTVisitor;

public class SQLServerCreateUserStatement extends SQLServerStatementImpl implements SQLCreateStatement {
    private SQLExpr user;
    private SQLExpr login;

    public SQLServerCreateUserStatement() {

    }

    public SQLExpr getUser() {
        return user;
    }

    public void setUser(SQLExpr user) {
        this.user = user;
    }

    public SQLExpr getLogin() {
        return login;
    }

    public void setLogin(SQLExpr login) {
        if (login != null) {
            login.setParent(this);
        }
        this.login = login;
    }

    @Override
    public void accept0(SQLServerASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, user);
            acceptChild(visitor, login);
        }
        visitor.endVisit(this);
    }
}
