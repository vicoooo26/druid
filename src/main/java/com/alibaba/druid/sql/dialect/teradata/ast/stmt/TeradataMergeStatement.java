package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.teradata.ast.TeradataObjectImpl;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class TeradataMergeStatement extends TeradataStatementImpl {
    private SQLTableSource into;
    private String alias;
    private SQLTableSource using;
    private SQLExpr on;
    private MergeUpdateClause updateClause;
    private MergeInsertClause insertClause;

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, into);
            acceptChild(visitor, using);
            acceptChild(visitor, on);
            acceptChild(visitor, updateClause);
            acceptChild(visitor, insertClause);
        }
        visitor.endVisit(this);
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public SQLTableSource getInto() {
        return into;
    }

    public void setInto(SQLName into) {
        this.setInto(new SQLExprTableSource(into));
    }

    public void setInto(SQLTableSource into) {
        if (into != null) {
            into.setParent(this);
        }
        this.into = into;
    }

    public SQLTableSource getUsing() {
        return using;
    }

    public void setUsing(SQLTableSource using) {
        this.using = using;
    }

    public SQLExpr getOn() {
        return on;
    }

    public void setOn(SQLExpr on) {
        this.on = on;
    }

    public MergeUpdateClause getUpdateClause() {
        return updateClause;
    }

    public void setUpdateClause(MergeUpdateClause updateClause) {
        this.updateClause = updateClause;
    }

    public MergeInsertClause getInsertClause() {
        return insertClause;
    }

    public void setInsertClause(MergeInsertClause insertClause) {
        this.insertClause = insertClause;
    }


    public static class MergeUpdateClause extends TeradataObjectImpl {

        private List<SQLUpdateSetItem> items = new ArrayList<SQLUpdateSetItem>();
        private SQLExpr where;
        private SQLExpr deleteWhere;

        public List<SQLUpdateSetItem> getItems() {
            return items;
        }

        public void addItem(SQLUpdateSetItem item) {
            if (item != null) {
                item.setParent(this);
            }
            this.items.add(item);
        }

        public SQLExpr getWhere() {
            return where;
        }

        public void setWhere(SQLExpr where) {
            this.where = where;
        }

        public SQLExpr getDeleteWhere() {
            return deleteWhere;
        }

        public void setDeleteWhere(SQLExpr deleteWhere) {
            this.deleteWhere = deleteWhere;
        }

        @Override
        public void accept0(TeradataASTVisitor visitor) {
            if (visitor.visit(this)) {
                acceptChild(visitor, items);
                acceptChild(visitor, where);
                acceptChild(visitor, deleteWhere);
            }
            visitor.endVisit(this);
        }

    }

    public static class MergeInsertClause extends TeradataObjectImpl {

        private List<SQLExpr> columns = new ArrayList<SQLExpr>();
        private List<SQLExpr> values = new ArrayList<SQLExpr>();
        private SQLExpr where;

        @Override
        public void accept0(TeradataASTVisitor visitor) {
            if (visitor.visit(this)) {
                acceptChild(visitor, columns);
                acceptChild(visitor, columns);
                acceptChild(visitor, columns);
            }
            visitor.endVisit(this);
        }

        public List<SQLExpr> getColumns() {
            return columns;
        }

        public void setColumns(List<SQLExpr> columns) {
            this.columns = columns;
        }

        public List<SQLExpr> getValues() {
            return values;
        }

        public void setValues(List<SQLExpr> values) {
            this.values = values;
        }

        public SQLExpr getWhere() {
            return where;
        }

        public void setWhere(SQLExpr where) {
            this.where = where;
        }

    }

}
