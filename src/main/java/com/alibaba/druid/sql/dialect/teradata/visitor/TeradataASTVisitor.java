package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.*;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface TeradataASTVisitor extends SQLASTVisitor {
    boolean visit(TeradataCreateDatabaseStatement x);

    void endVisit(TeradataCreateDatabaseStatement x);

//    boolean visit(TeradataAnalyticWindowing x);
//
//    void endVisit(TeradataAnalyticWindowing x);
//
//    boolean visit(TeradataAnalytic x);

//    void endVisit(TeradataAnalytic x);

    boolean visit(SQLSelectQueryBlock x);

    void endVisit(SQLSelectQueryBlock x);

    boolean visit(TeradataSelect x);

    void endVisit(TeradataSelect x);

//
//    boolean visit(TeradataIntervalExpr x);
//
//    void endVisit(TeradataIntervalExpr x);

    boolean visit(SQLSubqueryTableSource x);

    void endVisit(SQLSubqueryTableSource x);

//    boolean visit(TeradataDateExpr x);
//
//    void endVisit(TeradataDateExpr x);
//
//    boolean visit(TeradataFormatExpr x);
//
//    void endVisit(TeradataFormatExpr x);
//
//    boolean visit(TeradataExtractExpr x);
//
//    void endVisit(TeradataExtractExpr x);
//
//    boolean visit(TeradataDateTimeDataType x);
//
//    void endVisit(TeradataDateTimeDataType x);

    boolean visit(TeradataCreateTableStatement x);

    void endVisit(TeradataCreateTableStatement x);

    boolean visit(SQLCreateProcedureStatement x);

    void endVisit(SQLCreateProcedureStatement x);

    boolean visit(TeradataCollectStatement x);

    void endVisit(TeradataCollectStatement x);

    boolean visit(TeradataHelpStatement x);

    void endVisit(TeradataHelpStatement x);

    boolean visit(TeradataIndex x);

    void endVisit(TeradataIndex x);

    boolean visit(TeradataUpdateStatement x);

    void endVisit(TeradataUpdateStatement x);

    boolean visit(TeradataDeleteStatement x);

    void endVisit(TeradataDeleteStatement x);

    boolean visit(TeradataMergeStatement x);

    void endVisit(TeradataMergeStatement x);

    boolean visit(TeradataMergeStatement.MergeUpdateClause x);

    void endVisit(TeradataMergeStatement.MergeUpdateClause x);

    boolean visit(TeradataMergeStatement.MergeInsertClause x);

    void endVisit(TeradataMergeStatement.MergeInsertClause x);

    boolean visit(TeradataSelectStatement x);

    void endVisit(TeradataSelectStatement x);

}
