package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;

public class TeradataCreateTableStatement extends SQLCreateTableStatement {
    private boolean withData;
    private boolean withNoData;
    private boolean onCommit;
    private boolean preserveRows;
    protected boolean uniqueIndex = false;
    protected List<SQLConstraint> constraints = new ArrayList<SQLConstraint>();

    public TeradataCreateTableStatement() {
        super(JdbcConstants.TERADATA);
    }

    public boolean isUniqueIndex() {
        return uniqueIndex;
    }

    public void setUniqueIndex(boolean uniqueIndex) {
        this.uniqueIndex = uniqueIndex;
    }

    public void setConstraints(SQLConstraint sqlConstraint) {
        constraints.add(sqlConstraint);
    }

    public List<SQLConstraint> getConstraints() {
        return constraints;
    }

    public List<SQLTableElement> getTableElementList() {
        return tableElementList;
    }

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

    protected void accept0(SQLASTVisitor visitor) {
        accept0((TeradataASTVisitor) visitor);
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, select);
        }
        visitor.endVisit(this);
    }
}
