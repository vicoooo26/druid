package com.alibaba.druid.sql.dialect.teradata.visitor;

import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.*;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement.MergeInsertClause;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement.MergeUpdateClause;

public class TeradataASTVisitorAdapter extends SQLASTVisitorAdapter implements TeradataASTVisitor {
//    @Override
//    public boolean visit(TeradataAnalyticWindowing x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataAnalyticWindowing x) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean visit(TeradataAnalytic x) {
//        // TODO Auto-generated method stub
//        return false;
//    }

//    @Override
//    public boolean visit(TeradataIntervalExpr x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataIntervalExpr x) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean visit(TeradataDateExpr x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataDateExpr x) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean visit(TeradataFormatExpr x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataFormatExpr x) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean visit(TeradataExtractExpr x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataExtractExpr x) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean visit(TeradataDateTimeDataType x) {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public void endVisit(TeradataDateTimeDataType x) {
//        // TODO Auto-generated method stub
//
//    }

    @Override
    public boolean visit(TeradataCreateTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataCreateTableStatement x) {

    }

    @Override
    public boolean visit(TeradataIndex x) {
        return true;
    }

    @Override
    public void endVisit(TeradataIndex x) {

    }

    @Override
    public boolean visit(TeradataCollectStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataCollectStatement x) {

    }

    @Override
    public boolean visit(TeradataHelpStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataHelpStatement x) {

    }

    @Override
    public boolean visit(TeradataCreateDatabaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataCreateDatabaseStatement x) {

    }

    @Override
    public boolean visit(TeradataUpdateStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataUpdateStatement x) {

    }

    @Override
    public boolean visit(TeradataDeleteStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataDeleteStatement x) {

    }

    @Override
    public boolean visit(TeradataMergeStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataMergeStatement x) {

    }

    @Override
    public boolean visit(MergeUpdateClause x) {
        return true;
    }

    @Override
    public void endVisit(MergeUpdateClause x) {

    }

    @Override
    public boolean visit(MergeInsertClause x) {
        return true;
    }

    @Override
    public void endVisit(MergeInsertClause x) {

    }

    @Override
    public boolean visit(TeradataSelectStatement x) {
        return true;
    }

    @Override
    public void endVisit(TeradataSelectStatement x) {

    }

    @Override
    public boolean visit(TeradataSelect x) {
        return true;
    }

    @Override
    public void endVisit(TeradataSelect x) {

    }

    @Override
    public boolean visit(SQLCreateProcedureStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLCreateProcedureStatement x) {

    }

}
