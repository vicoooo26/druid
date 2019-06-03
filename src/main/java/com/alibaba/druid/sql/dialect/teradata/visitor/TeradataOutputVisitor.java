package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.*;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement.MergeInsertClause;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement.MergeUpdateClause;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import org.codehaus.janino.IClass;

import java.util.List;


public class TeradataOutputVisitor extends SQLASTOutputVisitor implements TeradataASTVisitor {
    public TeradataOutputVisitor(Appendable appender) {
        super(appender);
    }

    @Override
    public boolean visit(TeradataCreateDatabaseStatement x) {
        print0(ucase ? "CREATE DATABASE " : "create database ");
        printTableSourceExpr(x.getName());

        if (!(x.getSpool() == null && x.getPerm() == null)) {
            println();
            print0(ucase ? "AS " : "as ");
            println();
            if (x.getPerm() != null) {
                x.getPerm().accept(this);
            }

            if (x.getSpool() != null) {
                print(", ");
                x.getSpool().accept(this);
            }
        }
        return false;
    }

    @Override
    public boolean visit(TeradataCollectStatement x) {

        print0(ucase ? "COLLECT STATISTICS " : "collect statistics ");

        if (x.getIndexList() != null && x.getIndexList().size() > 0) {
            print0(ucase ? "INDEX " : "index ");
            print0("(");
            for (SQLName sqlName : x.getIndexList()) {
                sqlName.accept(this);
            }
            print0(") ");

        }

        if (x.getColumnList() != null && x.getColumnList().size() > 0) {
            print0(ucase ? "COLUMN " : "column ");
            print0("(");
            for (SQLName sqlName : x.getColumnList()) {
                sqlName.accept(this);
            }
            print0(") ");

        }
        println();
        if (x.getOn() != null) {
            print0(ucase ? "ON " : "on ");
            x.getOn().accept(this);
        }
        return false;
    }

    @Override
    public void endVisit(TeradataCollectStatement x) {

    }

    @Override
    public boolean visit(TeradataHelpStatement x) {

        print0(ucase ? "HELP " + x.getType() + " " : "help " + x.getType() + " ");
        x.getTarget().accept(this);
        if (x.getOpts() != null) {
            String opts = ((SQLIdentifierExpr) x.getOpts()).getName();
            print0(ucase ? " " + opts.toUpperCase() : " " + opts.toLowerCase());
        }
        return false;
    }

    @Override
    public void endVisit(TeradataHelpStatement x) {

    }

    @Override
    public boolean visit(TeradataShowStatement x) {

        print0(ucase ? "SHOW " + x.getType() + " " : "show " + x.getType() + " ");
        x.getTarget().accept(this);

        return false;
    }

    @Override
    public void endVisit(TeradataShowStatement x) {

    }


    @Override
    public void endVisit(TeradataCreateDatabaseStatement x) {

    }


    @Override
    public boolean visit(TeradataCreateTableStatement x) {
        print0(ucase ? "CREATE " : "create ");

        final SQLCreateTableStatement.Type tableType = x.getType();
        if (SQLCreateTableStatement.Type.SET.equals(tableType)) {
            print0(ucase ? "SET " : "set ");
        } else if (SQLCreateTableStatement.Type.MULTISET.equals(tableType)) {
            print0(ucase ? "MULTISET " : "multiset ");
        }
        print0(ucase ? "TABLE " : "table ");

        if (x.isIfNotExists()) {
            print0(ucase ? "IF NOT EXISTS " : "if not exists ");
        }

        printTableSourceExpr(x.getName());

        if (x.isFallback()) {
            println(ucase ? ",FALLBACK," : ", fallback,");
        } else {
            println(ucase ? ",NO FALLBACK," : ",no fallback,");
        }
        if (x.isBeforeJournal()) {
            println(ucase ? "BEFORE JOURNAL," : ",before journal,");
        } else {
            println(ucase ? "NO BEFORE JOURNAL," : ",no before journal,");
        }
        if (x.isAfterJournal()) {
            println(ucase ? "AFTER JOURNAL," : ",after journal,");
        } else {
            println(ucase ? "NO AFTER JOURNAL," : ",no after journal,");
        }

        if (x.getChecksum() != null) {
            print0(ucase ? "CHECKSUM = " : ", checksum = ");
            print0(x.getChecksum().toString());
            println(",");
        }

        if (x.isMergeBlockRatio()) {
            println(ucase ? "DEFAULT MERGEBLOCKRATIO," : "default mergeblockratio,");
        } else {
            println(ucase ? "NO MERGEBLOCKRATIO," : "no mergeblockratio,");
        }
        if (x.getMap() != null) {
            print0(ucase ? "MAP = " : "map = ");
            print0(x.getMap().toString());
        }

        printTableElements(x.getTableElementList());

        SQLExprTableSource inherits = x.getInherits();
        if (inherits != null) {
            print0(ucase ? " INHERITS (" : " inherits (");
            inherits.accept(this);
            print(')');
        }

        SQLName storedAs = x.getStoredAs();
        if (storedAs != null) {
            print0(ucase ? " STORE AS " : " store as ");
            printExpr(storedAs);
        }

        if (x.isWithData()) {
            println();
            print0(ucase ? "WITH DATA" : "with data");
        }

        if (x.isWithNoData()) {
            println();
            print0(ucase ? "WITH NO DATA" : "with no data");
        }

        if (x.isOnCommit()) {
            println();
            print0(ucase ? "ON COMMIT" : "on commit");
        }

        if (x.isPreserveRows()) {
            println();
            print0(ucase ? "PRESERVE ROWS" : "preserve rows");
        }

        if (x.getSelect() != null) {
            println();
            print0(ucase ? "AS" : "as");
            println();
            x.getSelect().accept(this);
        }

        if (x.getConstraints() != null && x.getConstraints().size() > 0) {
            println();
            if (x.isUniqueIndex()) {
                print0(ucase ? "UNIQUE PRIMARY " : "unique primary ");
            } else {
                print0(ucase ? "PRIMARY" : "primary");
            }

            List<SQLConstraint> constraints = x.getConstraints();
            for (SQLConstraint sqlConstraint : constraints) {
                print0(ucase ? "INDEX (" : "index (");
                printAndAccept(((SQLUnique) sqlConstraint).getColumns(), ", ");
                print(')');
                return false;
            }
        }
        return false;
    }

    @Override
    public void endVisit(TeradataCreateTableStatement x) {
        // TODO Auto-generated method stub

    }


    @Override
    public boolean visit(TeradataReplaceFunctionStatement x) {
        print0(ucase ? "REPLACE FUNCTION " : "replace function ");
        x.getName().accept(this);

        int paramSize = x.getParameters().size();

        if (paramSize > 0) {
            this.indentCount++;
            println("(");

            for (int i = 0; i < paramSize; ++i) {
                if (i != 0) {
                    print0(", ");
                    println();
                }
                SQLParameter param = x.getParameters().get(i);
                param.accept(this);
            }

            this.indentCount--;
            println();
            println(")");
        }
        if (x.getReturnDataType() != null
        ) {
            print0(ucase ? "RETURNS " : "returns ");
            x.getReturnDataType().accept(this);
            println();
        }
        if (x.isLanguageSql()) {
            println(ucase ? "LANGUAGE SQL " : "language sql ");
        }

        if (x.isContainsSql()) {
            println(ucase ? "CONTAINS SQL" : "contains sql");
        }

        if (x.isDeterministic()) {
            println(ucase ? "DETERMINISTIC " : "deterministic ");
        }
        SQLExpr sqlSecurity = x.getSqlSecurity();
        if (sqlSecurity != null) {
            print0(ucase ? "SQL SECURITY " : "sql security ");
            sqlSecurity.accept(this);
            println();
        }
        SQLExpr collation = x.getCollation();
        if (collation != null) {
            print0(ucase ? "COLLATION " : "collation ");
            collation.accept(this);
            println();
        }
        SQLExpr inline = x.getInline();
        if (inline != null) {
            print0(ucase ? "INLINE TYPE " : "inline type ");
            inline.accept(this);
        }
        SQLStatement returnBlock = x.getBlock();
        if (returnBlock != null) {
            println();
            returnBlock.accept(this);
        }
        return false;
    }

    @Override
    public void endVisit(TeradataReplaceFunctionStatement x) {

    }

    @Override
    public boolean visit(TeradataReturnStatement x) {
        String str = x.getReturnBlock();
        if (str != null) {
            print0(ucase ? "RETURN " : "return ");
            println("(");
            println(str);
            print(")");
        }
        return false;
    }

    @Override
    public void endVisit(TeradataReturnStatement x) {

    }


    @Override
    public boolean visit(SQLCreateProcedureStatement x) {
        boolean create = x.isCreate();
        if (!create) {
            print0(ucase ? "PROCEDURE " : "procedure ");
        } else if (x.isOrReplace()) {
            print0(ucase ? "CREATE OR REPLACE PROCEDURE " : "create or replace procedure ");
        } else {
            print0(ucase ? "CREATE PROCEDURE " : "create procedure ");
        }
        x.getName().accept(this);

        int paramSize = x.getParameters().size();

        if (paramSize > 0) {
            this.indentCount++;
            println("(");

            for (int i = 0; i < paramSize; ++i) {
                if (i != 0) {
                    print0(", ");
                    println();
                }
                SQLParameter param = x.getParameters().get(i);
                param.accept(this);
            }

            this.indentCount--;
            println();
            println(")");
        }

        SQLStatement block = x.getBlock();
        if (block != null) {
            block.accept(this);
        }
        x.setAfterSemi(false);
        return false;
    }

    @Override
    public void endVisit(SQLCreateProcedureStatement x) {
    }


    @Override
    public boolean visit(TeradataUpdateStatement x) {
        print0(ucase ? "UPDATE " : "update ");

        x.getTableSource().accept(this);

        if (x.getFrom() != null) {
            println();
            print0(ucase ? "FROM " : "from ");
            x.getFrom().accept(this);
        }

        println();

        print0(ucase ? "SET " : "set ");
        for (int i = 0, size = x.getItems().size(); i < size; ++i) {
            if (i != 0) {
                print0(", ");
            }
            x.getItems().get(i).accept(this);
        }

        if (x.getWhere() != null) {
            println();
            print0(ucase ? "WHERE " : "where ");
            incrementIndent();
            x.getWhere().setParent(x);
            x.getWhere().accept(this);
            decrementIndent();
        }

        return false;
    }

    @Override
    public void endVisit(TeradataUpdateStatement x) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean visit(TeradataSelectStatement x) {
        List<SQLCommentHint> headHints = x.getHeadHintsDirect();
        if (headHints != null) {
            for (SQLCommentHint hint : headHints) {
                hint.accept(this);
                println();
            }
        }

        TeradataSelect select = x.getSelect();
        this.visit(select);

        return false;
    }

    @Override
    public void endVisit(TeradataSelectStatement x) {

    }

    @Override
    public boolean visit(TeradataSelect x) {
        SQLWithSubqueryClause withSubQuery = x.getWithSubQuery();
        if (withSubQuery != null) {
            withSubQuery.accept(this);
            println();
        }

        printQuery(x.getQuery());

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            println();
            orderBy.accept(this);
        }

        if (x.getHintsSize() > 0) {
            printAndAccept(x.getHints(), "");
        }

        if (x.getSample() != null) {
            print0(ucase ? " SAMPLE " : " sample ");
            x.getSample().accept(this);
        }

        return false;

    }

    @Override
    public void endVisit(TeradataSelect x) {

    }

    @Override
    public boolean visit(TeradataDeleteStatement x) {
        print0(ucase ? "DELETE " : "delete ");

        if (x.getFrom() == null) {
            print0(ucase ? "FROM " : "from ");
            x.getTableSource().accept(this);
        } else {
            x.getTableSource().accept(this);
            println();
            print0(ucase ? "FROM " : "from ");
            x.getFrom().accept(this);
        }

        if (x.getUsing() != null) {
            println();
            print0(ucase ? "USING " : "using ");
            x.getUsing().accept(this);
        }

        if (x.getWhere() != null) {
            println();
            incrementIndent();
            print0(ucase ? "WHERE " : "where ");
            x.getWhere().setParent(x);
            x.getWhere().accept(this);
            decrementIndent();
        }

        if (x.getOrderBy() != null) {
            println();
            x.getOrderBy().accept(this);
        }

        if (x.getAll() != null) {
            print(" ");
            x.getAll().accept(this);
        }

        return false;
    }

    @Override
    public void endVisit(TeradataDeleteStatement x) {

    }

    @Override
    public boolean visit(TeradataMergeStatement x) {

        print0(ucase ? "MERGE " : "merge ");

        print0(ucase ? "INTO " : "into ");
        x.getInto().accept(this);

        if (x.getAlias() != null) {
            print(' ');
            print0(x.getAlias());
        }

        println();
        print0(ucase ? "USING " : "using ");
        x.getUsing().accept(this);

        print0(ucase ? " ON (" : " on (");
        x.getOn().accept(this);
        print0(") ");

        if (x.getUpdateClause() != null) {
            println();
            x.getUpdateClause().accept(this);
        }

        if (x.getInsertClause() != null) {
            println();
            x.getInsertClause().accept(this);
        }

        return false;
    }

    @Override
    public void endVisit(TeradataMergeStatement x) {

    }

    @Override
    public boolean visit(MergeUpdateClause x) {
        print0(ucase ? "WHEN MATCHED THEN UPDATE SET " : "when matched then update set ");
        printAndAccept(x.getItems(), ", ");
        if (x.getWhere() != null) {
            incrementIndent();
            println();
            print0(ucase ? "WHERE " : "where ");
            x.getWhere().setParent(x);
            x.getWhere().accept(this);
            decrementIndent();
        }

        if (x.getDeleteWhere() != null) {
            incrementIndent();
            println();
            print0(ucase ? "DELETE WHERE " : "delete where ");
            x.getDeleteWhere().setParent(x);
            x.getDeleteWhere().accept(this);
            decrementIndent();
        }

        return false;
    }

    @Override
    public void endVisit(MergeUpdateClause x) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean visit(MergeInsertClause x) {
        print0(ucase ? "WHEN NOT MATCHED THEN INSERT" : "when not matched then insert");
        if (x.getColumns().size() > 0) {
            print(' ');
            printAndAccept(x.getColumns(), ", ");
        }
        print0(ucase ? " VALUES (" : " values (");
        printAndAccept(x.getValues(), ", ");
        print(')');
        if (x.getWhere() != null) {
            incrementIndent();
            println();
            print0(ucase ? "WHERE " : "where ");
            x.getWhere().setParent(x);
            x.getWhere().accept(this);
            decrementIndent();
        }

        return false;
    }

    @Override
    public void endVisit(MergeInsertClause x) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean visit(TeradataIndex x) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void endVisit(TeradataIndex x) {
        // TODO Auto-generated method stub

    }

}
