package com.alibaba.druid.sql.dialect.teradata.ast;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public abstract class TeradataObjectImpl extends SQLObjectImpl implements TeradataObject {

    protected void accept0(SQLASTVisitor visitor) {
        this.accept0((TeradataASTVisitor) visitor);
    }

    public abstract void accept0(TeradataASTVisitor visitor);

    public String toString() {
        return "";
    }

}
