package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;

public class TeradataReplaceFunctionStatement extends SQLAlterFunctionStatement {
    private SQLStatement block;
    private List<SQLParameter> parameters = new ArrayList<SQLParameter>();
    private SQLDataType returnDataType;
    private SQLExpr inline;
    private SQLExpr collation;
    private boolean deterministic = false;

    public TeradataReplaceFunctionStatement() {
        this.dbType = JdbcConstants.TERADATA;
    }

    public SQLExpr getCollation() {
        return collation;
    }

    public void setCollation(SQLExpr collation) {
        this.collation = collation;
    }

    public SQLExpr getInline() {
        return inline;
    }

    public void setInline(SQLExpr inline) {
        this.inline = inline;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    public List<SQLParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<SQLParameter> parameters) {
        this.parameters = parameters;
    }

    public SQLStatement getBlock() {
        return block;
    }

    public void setBlock(SQLStatement block) {
        if (block != null) {
            block.setParent(this);
        }
        this.block = block;
    }

    public SQLDataType getReturnDataType() {
        return returnDataType;
    }

    public void setReturnDataType(SQLDataType returnDataType) {
        if (returnDataType != null) {
            returnDataType.setParent(this);
        }
        this.returnDataType = returnDataType;
    }

    public void accept0(SQLASTVisitor visitor) {
        accept0((TeradataASTVisitor) visitor);
    }

    public void accept0(TeradataASTVisitor visitor) {
        if (visitor.visit(this)) {
//            this.acceptChild(visitor, select);
        }
        visitor.endVisit(this);
    }
}
