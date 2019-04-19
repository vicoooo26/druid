package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class TeradataCreateFunctionStatement extends SQLCreateFunctionStatement {
    private boolean withData;
    private boolean withNoData;
    private boolean onCommit;
    private boolean preserveRows;

    public boolean isWithData() {
        return withData;
    }

    public void setWithData(boolean withData) {
        this.withData = withData;
    }

    public boolean isWithNoData() {
        return withNoData;
    }

    public void setWithNoData(boolean withNoData) {
        this.withNoData = withNoData;
    }

    public boolean isOnCommit() {
        return onCommit;
    }

    public void setOnCommit(boolean onCommit) {
        this.onCommit = onCommit;
    }

    public boolean isPreserveRows() {
        return preserveRows;
    }

    public void setPreserveRows(boolean preserveRows) {
        this.preserveRows = preserveRows;
    }

    public void accept0(SQLASTVisitor visitor) {
        accept0((TeradataASTVisitor) visitor);
    }

//    public void accept0(TeradataASTVisitor visitor) {
//        if (visitor.visit(this)) {
//            this.acceptChild(visitor, select);
//        }
//        visitor.endVisit(this);
//    }
}
