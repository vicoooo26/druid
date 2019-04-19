package com.alibaba.druid.sql.dialect.sqlserver.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerStatementImpl;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerASTVisitor;

public class SQLServerCreateSchemaStatement extends SQLServerStatementImpl implements SQLCreateStatement {

    private SQLExpr schema;

    public SQLServerCreateSchemaStatement() {

    }

    public SQLExpr getSchema() {
        return schema;
    }

    public void setSchema(SQLExpr schema) {
        this.schema = schema;
    }

    @Override
    public void accept0(SQLServerASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, schema);
        }
        visitor.endVisit(this);
    }
}
