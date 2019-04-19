package com.alibaba.druid.sql.dialect.sqlserver.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerStatementImpl;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerASTVisitor;

public class SQLServerCreateLoginStatement extends SQLServerStatementImpl implements SQLCreateStatement {
    private SQLExpr login;
    private SQLExpr password;

    public SQLServerCreateLoginStatement() {
    }

    public SQLExpr getLogin() {
        return login;
    }

    public void setLogin(SQLExpr login) {
        this.login = login;
    }

    public SQLExpr getPassword() {
        return password;
    }

    public void setPassword(SQLExpr password) {
        if (password != null) {
            password.setParent(this);
        }
        this.password = password;
    }

    @Override
    public void accept0(SQLServerASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, login);
            acceptChild(visitor, password);
        }
        visitor.endVisit(this);
    }
}
