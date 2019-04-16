package com.alibaba.druid.sql.dialect.db2.ast;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2ASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public abstract class DB2ObjectImpl extends SQLObjectImpl implements DB2Object {
    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof DB2ASTVisitor) {
            this.accept0((DB2ASTVisitor) visitor);
        }
    }

    public abstract void accept0(DB2ASTVisitor visitor);

    public String toString() {
        return SQLUtils.toDB2String(this);
    }
}
