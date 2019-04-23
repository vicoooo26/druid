package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLSelect;

public class TeradataSelect extends SQLSelect {
    protected SQLObject sample;
    protected SQLObject top;

    public SQLObject getTop() {
        return top;
    }

    public void setTop(SQLObject top) {
        this.top = top;
    }

    public SQLObject getSample() {
        return sample;
    }

    public void setSample(SQLObject sample) {
        this.sample = sample;
    }

}
