package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;

public class TeradataShowStatement extends TeradataStatementImpl {

    private SQLObject target;
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public SQLObject getTarget() {
        return target;
    }

    public void setTarget(SQLObject target) {
        this.target = target;
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, target);
        }
        visitor.endVisit(this);
    }

}
