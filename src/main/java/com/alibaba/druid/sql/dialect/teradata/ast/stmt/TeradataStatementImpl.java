package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

public abstract class TeradataStatementImpl extends SQLStatementImpl implements TeradataStatement {
    public TeradataStatementImpl() {
        super(JdbcConstants.TERADATA);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof TeradataASTVisitor) {
            accept0((TeradataASTVisitor) visitor);
        }
    }

    public abstract void accept0(TeradataASTVisitor visitor);

    public String toString() {
        return SQLUtils.toTeradataString(this);
    }
}
