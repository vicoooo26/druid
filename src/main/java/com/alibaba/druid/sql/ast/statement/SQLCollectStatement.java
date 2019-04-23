package com.alibaba.druid.sql.ast.statement;

import com.alibaba.druid.sql.ast.SQLStatementImpl;

public class SQLCollectStatement extends SQLStatementImpl {
    public SQLCollectStatement() {

    }

    public SQLCollectStatement(String dbType) {
        super (dbType);
    }
}
