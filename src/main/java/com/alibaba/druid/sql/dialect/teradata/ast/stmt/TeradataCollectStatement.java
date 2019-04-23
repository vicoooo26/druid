package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;

public class TeradataCollectStatement extends TeradataStatementImpl {

    protected List<SQLName> indexList = new ArrayList<>();
    protected List<SQLName> columnList = new ArrayList<>();
    protected SQLObject on;

    public SQLObject getOn() {
        return on;
    }

    public void setOn(SQLObject on) {
        this.on = on;
    }

    public List<SQLName> getColumnList() {
        return columnList;
    }

    public void addColumn(SQLName column) {
        this.columnList.add(column);
    }

    public List<SQLName> getIndexList() {
        return indexList;
    }

    public void addIndex(SQLName index) {
        this.indexList.add(index);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof TeradataASTVisitor) {
            accept0(((TeradataASTVisitor) visitor));
        }
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, on);
            acceptChild(visitor, columnList);
            acceptChild(visitor, indexList);
        }
        visitor.endVisit(this);
    }


    @Override
    public List<SQLObject> getChildren() {
        List<SQLObject> children = new ArrayList<SQLObject>();
        if (on != null) {
            children.add(on);
        }
        children.addAll(indexList);
        children.addAll(columnList);
        return children;
    }
}
