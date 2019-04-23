package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableConstraint;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.ast.statement.SQLUniqueConstraint;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class TeradataIndex extends SQLUnique implements SQLUniqueConstraint, SQLTableConstraint {
    private SQLName name;
    private List<SQLSelectOrderByItem> columns = new ArrayList<SQLSelectOrderByItem>();

    private boolean isUnique;
    private boolean isPrimary;
    private boolean isUniqueAndPrimary;

    @Override
    public boolean containsColumn(String column) {
        return false;
    }

    @Override
    public List<SQLSelectOrderByItem> getColumns() {
        return columns;
    }

    @Override
    public SQLName getName() {
        return name;
    }

    @Override
    public void setName(SQLName name) {
        this.name = name;
    }

    @Override
    public void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof TeradataASTVisitor) {
            accept0((TeradataASTVisitor) visitor);
        }
        visitor.endVisit(this);
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
            acceptChild(visitor, columns);
        }
        visitor.endVisit(this);
    }

    public boolean isUniqueIndex() {
        return isUnique;
    }

    public void setUniqueIndex(boolean isUnique) {
        this.isUnique = isUnique;
    }

    public boolean isPrimaryIndex() {
        return isPrimary;
    }

    public void setPrimaryIndex(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public boolean isUniqueAndPrimaryIndex() {
        return isUniqueAndPrimary;
    }

    public void setUniqueAndPrimaryIndex(boolean isUniqueAndPrimary) {
        this.isUniqueAndPrimary = isUniqueAndPrimary;
    }
}
