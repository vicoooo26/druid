package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.statement.SQLReturnStatement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

public class TeradataReturnStatement extends SQLReturnStatement {
    private String returnBlock;

    public TeradataReturnStatement() {
        super(JdbcConstants.TERADATA);
    }

    public String getReturnBlock() {
        return returnBlock;
    }

    public void setReturnBlock(String returnBlock) {
        this.returnBlock = returnBlock;
    }

    public void accept0(SQLASTVisitor visitor) {
        accept0((TeradataASTVisitor) visitor);
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
//            this.acceptChild(visitor, returnBlock);
        }
        visitor.endVisit(this);
    }

}
