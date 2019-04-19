package com.alibaba.druid.sql.dialect.db2.ast.stmt;

import com.alibaba.druid.sql.dialect.db2.ast.DB2ObjectImpl;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2ASTVisitor;

public abstract class DB2CreateRestriction extends DB2ObjectImpl {
    public DB2CreateRestriction() {
    }

    public static class DefinitionOnly extends DB2CreateRestriction {

        public DefinitionOnly() {

        }

        public void accept0(DB2ASTVisitor visitor) {
            visitor.visit(this);

            visitor.endVisit(this);
        }

        public DefinitionOnly clone() {
            DefinitionOnly x = new DefinitionOnly();
            return x;
        }
    }

}
