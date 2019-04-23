package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.*;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;


public class TeradataStatementParser extends SQLStatementParser {
    public TeradataStatementParser(String sql) {
        super(new TeradataExprParser(sql));
    }

    public TeradataStatementParser(Lexer lexer) {
        super(new TeradataExprParser(lexer));
    }

    public TeradataExprParser getExprParser() {
        return (TeradataExprParser) exprParser;
    }

    public TeradataSelectParser createSQLSelectParser() {
        return new TeradataSelectParser(this.exprParser);
    }


    public SQLStatement parseCreate() {
        char markChar = lexer.current();
        int markBp = lexer.bp();

        List<String> comments = null;
        if (lexer.isKeepComments() && lexer.hasComment()) {
            comments = lexer.readAndResetComments();
        }

        accept(Token.CREATE);

        boolean replace = false;
        if (lexer.token() == Token.OR) {
            lexer.nextToken();
            accept(Token.REPLACE);
            replace = true;
        }

        if (lexer.token() == Token.TABLE
                || lexer.token() == Token.VOLATILE
                || lexer.token() == Token.SET
                || lexer.token() == Token.MULTISET) {
            TeradataCreateTableParser parser = new TeradataCreateTableParser(this.exprParser);
            TeradataCreateTableStatement stmt = parser.parseCreateTable(false);
            return stmt;
        }

        if (lexer.token() == Token.DATABASE || lexer.token() == Token.SCHEMA) {
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            return parseCreateDatabase();
        }

        if (lexer.token() == Token.UNIQUE || lexer.token() == Token.INDEX || lexer.token() == Token.FULLTEXT
                || lexer.identifierEquals("SPATIAL")) {
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            return parseCreateIndex(false);
        }
//
//        if (lexer.token() == Token.USER) {
//            if (replace) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//            }
//            return parseCreateUser();
//        }
//
        if (lexer.token() == Token.VIEW || lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            return parseCreateView();
        }

        if (lexer.token() == Token.TRIGGER) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateTrigger();
        }

        // parse create procedure
        if (lexer.token() == Token.PROCEDURE) {
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            return parseCreateProcedure();
        }
//
//        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
////            Lexer.SavePoint savePoint = lexer.mark();
//            lexer.nextToken();
//            accept(Token.EQ);
//            this.getExprParser().userName();
//
//            if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
//                lexer.nextToken();
//                acceptIdentifier("SECURITY");
//                if (lexer.token() == Token.EQ) {
//                    lexer.nextToken();
//                }
//                lexer.nextToken();
//            }
//            if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//                return parseCreateEvent();
//            } else if (lexer.token() == Token.TRIGGER) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//                return parseCreateTrigger();
//            } else if (lexer.token() == Token.VIEW) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//                return parseCreateView();
//            } else if (lexer.token() == Token.FUNCTION) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//                return parseCreateFunction();
//            } else {
//                lexer.reset(markBp, markChar, Token.CREATE);
//                return parseCreateProcedure();
//            }
//        }
//
//        if (lexer.token() == Token.FUNCTION) {
//            if (replace) {
//                lexer.reset(markBp, markChar, Token.CREATE);
//            }
//            return parseCreateFunction();
//        }
//
//        if (lexer.identifierEquals(FnvHash.Constants.LOGFILE)) {
//            return parseCreateLogFileGroup();
//        }
//
//        if (lexer.identifierEquals(FnvHash.Constants.SERVER)) {
//            return parseCreateServer();
//        }
//
        if (lexer.token() == Token.TABLESPACE) {
//            return parseCreateTableSpace();
        }

        throw new ParserException("TODO " + lexer.info());
    }


    public TeradataSelectStatement parseSelect() {
        TeradataSelectParser selectParser = new TeradataSelectParser(this.exprParser);
        return new TeradataSelectStatement(selectParser.select(), JdbcConstants.TERADATA);
    }

//    public SQLCreateProcedureStatement parseCreateProcedure() {
//
//        if (lexer.token() == Token.PROCEDURE) {
////            if (replace) {
////                lexer.reset(markBp, markChar, Token.CREATE);
////            }
//        }
//        return null;
//
//    }

    /**
     * parse create procedure stmt
     */
    public SQLCreateProcedureStatement parseCreateProcedure() {
        /**
         * CREATE OR REPALCE PROCEDURE SP_NAME(parameter_list) BEGIN block_statement END
         */
        SQLCreateProcedureStatement stmt = new SQLCreateProcedureStatement();
        stmt.setDbType(dbType);

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();

            if (lexer.token() == Token.OR) {
                lexer.nextToken();
                accept(Token.REPLACE);
                stmt.setOrReplace(true);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.DEFINER)) {
            lexer.nextToken();
            accept(Token.EQ);
//            SQLName definer = this.getExprParser().userName();
//            stmt.setDefiner(definer);
        }

        accept(Token.PROCEDURE);

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LPAREN) {// match "("
            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
            accept(Token.RPAREN);// match ")"
        }

        for (; ; ) {
            if (lexer.identifierEquals(FnvHash.Constants.DETERMINISTIC)) {
                lexer.nextToken();
                stmt.setDeterministic(true);
                continue;
            }
            if (lexer.identifierEquals(FnvHash.Constants.CONTAINS) || lexer.token() == Token.CONTAINS) {
                lexer.nextToken();
                acceptIdentifier("SQL");
                stmt.setContainsSql(true);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
                lexer.nextToken();
                acceptIdentifier("SECURITY");
                SQLName authid = this.exprParser.name();
                stmt.setAuthid(authid);
            }

            break;
        }

        SQLStatement block;
        if (lexer.token() == Token.BEGIN) {
            block = this.parseBlock();
        } else {
            block = this.parseStatement();
        }

        stmt.setBlock(block);
        return stmt;
    }

    /**
     * parse create procedure parameters
     *
     * @param parameters
     */
    private void parserParameters(List<SQLParameter> parameters, SQLObject parent) {
        if (lexer.token() == Token.RPAREN) {
            return;
        }

        for (; ; ) {
            SQLParameter parameter = new SQLParameter();

            if (lexer.token() == Token.CURSOR) {
                lexer.nextToken();

                parameter.setName(this.exprParser.name());

                accept(Token.IS);
                SQLSelect select = this.createSQLSelectParser().select();

                SQLDataTypeImpl dataType = new SQLDataTypeImpl();
                dataType.setName("CURSOR");
                parameter.setDataType(dataType);

                parameter.setDefaultValue(new SQLQueryExpr(select));

            } else if (lexer.token() == Token.IN || lexer.token() == Token.OUT || lexer.token() == Token.INOUT) {

                if (lexer.token() == Token.IN) {
                    parameter.setParamType(SQLParameter.ParameterType.IN);
                } else if (lexer.token() == Token.OUT) {
                    parameter.setParamType(SQLParameter.ParameterType.OUT);
                } else if (lexer.token() == Token.INOUT) {
                    parameter.setParamType(SQLParameter.ParameterType.INOUT);
                }
                lexer.nextToken();

                parameter.setName(this.exprParser.name());

                parameter.setDataType(this.exprParser.parseDataType());
            } else {
                parameter.setParamType(SQLParameter.ParameterType.DEFAULT);// default parameter type is in
                parameter.setName(this.exprParser.name());
                parameter.setDataType(this.exprParser.parseDataType());

                if (lexer.token() == Token.COLONEQ) {
                    lexer.nextToken();
                    parameter.setDefaultValue(this.exprParser.expr());
                }
            }

            parameters.add(parameter);
            if (lexer.token() == Token.COMMA || lexer.token() == Token.SEMI) {
                lexer.nextToken();
            }

            if (lexer.token() != Token.BEGIN && lexer.token() != Token.RPAREN) {
                continue;
            }

            break;
        }
    }

    /**
     * parse procedure stmt block
     *
     * @param statementList
     */
    private void parseProcedureStatementList(List<SQLStatement> statementList) {
        parseProcedureStatementList(statementList, -1);
    }

    /**
     * parse procedure stmt block
     */
    private void parseProcedureStatementList(List<SQLStatement> statementList, int max) {

        for (; ; ) {
            if (max != -1) {
                if (statementList.size() >= max) {
                    return;
                }
            }

            if (lexer.token() == Token.EOF) {
                return;
            }
            if (lexer.token() == Token.END) {
                return;
            }
            if (lexer.token() == Token.ELSE) {
                return;
            }
            if (lexer.token() == (Token.SEMI)) {
                lexer.nextToken();
                continue;
            }
            if (lexer.token() == Token.WHEN) {
                return;
            }
            if (lexer.token() == Token.UNTIL) {
                return;
            }
            // select into
            if (lexer.token() == (Token.SELECT)) {
//                statementList.add(this.parseSelectInto());
                continue;
            }

            // update
            if (lexer.token() == (Token.UPDATE)) {
                statementList.add(parseUpdateStatement());
                continue;
            }

            // create
            if (lexer.token() == (Token.CREATE)) {
                statementList.add(parseCreate());
                continue;
            }

            // insert
            if (lexer.token() == Token.INSERT) {
                SQLStatement stmt = parseInsert();
                statementList.add(stmt);
                continue;
            }

            // delete
            if (lexer.token() == (Token.DELETE)) {
                statementList.add(parseDeleteStatement());
                continue;
            }

            // call
            if (lexer.token() == Token.LBRACE || lexer.identifierEquals("CALL")) {
                statementList.add(this.parseCall());
                continue;
            }

            // begin
            if (lexer.token() == Token.BEGIN) {
                statementList.add(this.parseBlock());
                continue;
            }

            if (lexer.token() == Token.VARIANT) {
                SQLExpr variant = this.exprParser.primary();
                if (variant instanceof SQLBinaryOpExpr) {
                    SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) variant;
                    if (binaryOpExpr.getOperator() == SQLBinaryOperator.Assignment) {
                        SQLSetStatement stmt = new SQLSetStatement(binaryOpExpr.getLeft(), binaryOpExpr.getRight(),
                                getDbType());
                        statementList.add(stmt);
                        continue;
                    }
                }
                accept(Token.COLONEQ);
                SQLExpr value = this.exprParser.expr();

                SQLSetStatement stmt = new SQLSetStatement(variant, value, getDbType());
                statementList.add(stmt);
                continue;
            }

            // select
            if (lexer.token() == Token.LPAREN) {
                char ch = lexer.current();
                int bp = lexer.bp();
                lexer.nextToken();

                if (lexer.token() == Token.SELECT) {
                    lexer.reset(bp, ch, Token.LPAREN);
                    statementList.add(this.parseSelect());
                    continue;
                } else {
                    throw new ParserException("TODO. " + lexer.info());
                }
            }
            // assign stmt
            if (lexer.token() == Token.SET) {
//                statementList.add(this.parseAssign());
                continue;
            }

            // while stmt
            if (lexer.token() == Token.WHILE) {
                SQLStatement stmt = this.parseWhile();
                statementList.add(stmt);
                continue;
            }

            // loop stmt
            if (lexer.token() == Token.LOOP) {
//                statementList.add(this.parseLoop());
                continue;
            }

            // if stmt
            if (lexer.token() == Token.IF) {
                statementList.add(this.parseIf());
                continue;
            }

            // case stmt
            if (lexer.token() == Token.CASE) {
                statementList.add(this.parseCase());
                continue;
            }

            // declare stmt
            if (lexer.token() == Token.DECLARE) {
                SQLStatement stmt = this.parseDeclare();
                statementList.add(stmt);
                continue;
            }

            // leave stmt
            if (lexer.token() == Token.LEAVE) {
                statementList.add(this.parseLeave());
                continue;
            }

            // iterate stmt
            if (lexer.token() == Token.ITERATE) {
//                statementList.add(this.parseIterate());
                continue;
            }

            // repeat stmt
            if (lexer.token() == Token.REPEAT) {
                statementList.add(this.parseRepeat());
                continue;
            }

            // open cursor
            if (lexer.token() == Token.OPEN) {
                statementList.add(this.parseOpen());
                continue;
            }

            // close cursor
            if (lexer.token() == Token.CLOSE) {
                statementList.add(this.parseClose());
                continue;
            }

            // fetch cursor into
            if (lexer.token() == Token.FETCH) {
                statementList.add(this.parseFetch());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.CHECKSUM)) {
//                statementList.add(this.parseChecksum());
                continue;
            }

            if (lexer.token() == Token.IDENTIFIER) {
                String label = lexer.stringVal();
                char ch = lexer.current();
                int bp = lexer.bp();
                lexer.nextToken();
                if (lexer.token() == Token.VARIANT && lexer.stringVal().equals(":")) {
                    lexer.nextToken();
                    if (lexer.token() == Token.LOOP) {
                        // parse loop stmt
//                        statementList.add(this.parseLoop(label));
                    } else if (lexer.token() == Token.WHILE) {
                        // parse while stmt with label
//                        statementList.add(this.parseWhile(label));
                    } else if (lexer.token() == Token.BEGIN) {
                        // parse begin-end stmt with label
//                        statementList.add(this.parseBlock(label));
                    } else if (lexer.token() == Token.REPEAT) {
                        // parse repeat stmt with label
//                        statementList.add(this.parseRepeat(label));
                    }
                    continue;
                } else {
                    lexer.reset(bp, ch, Token.IDENTIFIER);
                }

            }
            throw new ParserException("TODO, " + lexer.info());
        }

    }


    public TeradataCollectStatement parseCollect() {
        TeradataCollectStatement collect = new TeradataCollectStatement();
        collect.setDbType(dbType);

        accept(Token.COLLECT);
        if (lexer.token() == Token.STATISTICS) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.COLUMN) {
            lexer.nextToken();
            this.accept(Token.LPAREN);
            for (; ; ) {
                if (lexer.token() == Token.IDENTIFIER) {
                    SQLName column = this.exprParser.name();
                    collect.addColumn(column);
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                if (lexer.token() == Token.RPAREN) {
                    lexer.nextToken();
                    break;
                }
            }
        }
        if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
            this.accept(Token.LPAREN);
            for (; ; ) {
                if (lexer.token() == Token.IDENTIFIER) {
                    SQLName column = this.exprParser.name();
                    collect.addIndex(column);
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                if (lexer.token() == Token.RPAREN) {
                    lexer.nextToken();
                    break;
                }
            }
        }

        if (lexer.token() == Token.ON) {
            lexer.nextToken();
            collect.setOn(this.exprParser.expr());
        }

        return collect;

    }

    public TeradataHelpStatement parseHelp() {
        TeradataHelpStatement collect = new TeradataHelpStatement();
        collect.setDbType(dbType);

        accept(Token.HELP);

        collect.setType(lexer.token().name());

        lexer.nextToken();

        collect.setTarget(this.exprParser.expr());

        return collect;

    }


    public SQLInsertStatement parseInsert() {
        TeradataInsertStatement insertStatement = new TeradataInsertStatement();

        if (lexer.token() == Token.INSERT) {
            lexer.nextToken();

            if (lexer.token() == Token.INTO) {
                lexer.nextToken();
            }

            SQLName tableName = this.exprParser.name();
            insertStatement.setTableName(tableName);

            if (lexer.token() == Token.IDENTIFIER && !identifierEquals("VALUE")) {
                insertStatement.setAlias(lexer.stringVal());
                lexer.nextToken();
            }
        }

        int columnSize = 0;
        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            if (lexer.token() == (Token.SELECT)) {
                SQLSelect select = this.exprParser.createSelectParser().select();
                select.setParent(insertStatement);
                insertStatement.setQuery(select);
            } else {
                this.exprParser.exprList(insertStatement.getColumns(), insertStatement);
                columnSize = insertStatement.getColumns().size();
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.VALUES || identifierEquals("VALUE")) {
            lexer.nextTokenLParen();
            parseValueClause(insertStatement.getValuesList(), columnSize);
        } else if (lexer.token() == Token.SET) {
            lexer.nextToken();

            SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
            insertStatement.getValuesList().add(values);

            for (; ; ) {
                SQLName name = this.exprParser.name();
                insertStatement.addColumn(name);
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                } else {
                    accept(Token.COLONEQ);
                }
                values.addValue(this.exprParser.expr());

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

        } else if (lexer.token() == (Token.SELECT)
                || lexer.token() == (Token.SEL)) {
            SQLSelect select = this.exprParser.createSelectParser().select();
            select.setParent(insertStatement);
            insertStatement.setQuery(select);
        } else if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            SQLSelect select = this.exprParser.createSelectParser().select();
            select.setParent(insertStatement);
            insertStatement.setQuery(select);
            accept(Token.RPAREN);
        }

        return insertStatement;
    }

    public SQLBlockStatement parseBlock() {
        SQLBlockStatement block = new SQLBlockStatement();
        block.setDbType(dbType);

        accept(Token.BEGIN);
        List<SQLStatement> statementList = block.getStatementList();
        this.parseStatementList(statementList, -1, block);

        if (lexer.token() != Token.END
                && statementList.size() > 0
                && (statementList.get(statementList.size() - 1) instanceof SQLCommitStatement
                || statementList.get(statementList.size() - 1) instanceof SQLRollbackStatement)) {
            block.setEndOfCommit(true);
            return block;
        }
        accept(Token.END);

        return block;
    }

    private void parseValueClause(List<SQLInsertStatement.ValuesClause> valueClauseList, int columnSize) {
        for (; ; ) {
            if (lexer.token() != Token.LPAREN) {
                throw new ParserException("syntax error, expect ')'");
            }
            lexer.nextTokenValue();

            if (lexer.token() != Token.RPAREN) {
                List<SQLExpr> valueExprList;
                if (columnSize > 0) {
                    valueExprList = new ArrayList<SQLExpr>(columnSize);
                } else {
                    valueExprList = new ArrayList<SQLExpr>();
                }

                for (; ; ) {
                    SQLExpr expr;
                    if (lexer.token() == Token.LITERAL_INT) {
                        expr = new SQLIntegerExpr(lexer.integerValue());
                        lexer.nextTokenComma();
                    } else if (lexer.token() == Token.LITERAL_CHARS) {
                        expr = new SQLCharExpr(lexer.stringVal());
                        lexer.nextTokenComma();
                    } else if (lexer.token() == Token.LITERAL_NCHARS) {
                        expr = new SQLNCharExpr(lexer.stringVal());
                        lexer.nextTokenComma();
                    } else {
                        expr = exprParser.expr();
                    }

                    if (lexer.token() == Token.COMMA) {
                        valueExprList.add(expr);
                        lexer.nextTokenValue();
                        continue;
                    } else if (lexer.token() == Token.RPAREN) {
                        valueExprList.add(expr);
                        break;
                    } else {
                        expr = this.exprParser.primaryRest(expr);
                        if (lexer.token() != Token.COMMA && lexer.token() != Token.RPAREN) {
                            expr = this.exprParser.exprRest(expr);
                        }

                        valueExprList.add(expr);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        } else {
                            break;
                        }
                    }
                }

                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause(valueExprList);
                valueClauseList.add(values);
            } else {
                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause(new ArrayList<SQLExpr>(0));
                valueClauseList.add(values);
            }

            if (lexer.token() != Token.RPAREN) {
                throw new ParserException("syntax error");
            }

            if (!parseCompleteValues && valueClauseList.size() >= parseValuesSize) {
                lexer.skipToEOF();
                break;
            }

            lexer.nextTokenComma();
            if (lexer.token() == Token.COMMA) {
                lexer.nextTokenLParen();
                continue;
            } else {
                break;
            }
        }
    }


    public TeradataMergeStatement parseMerge() {
        accept(Token.MERGE);

        TeradataMergeStatement stmt = new TeradataMergeStatement();

        accept(Token.INTO);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            SQLSelect select = this.createSQLSelectParser().select();
            SQLSubqueryTableSource tableSource = new SQLSubqueryTableSource(select);
            stmt.setInto(tableSource);
            accept(Token.RPAREN);
        } else {
            stmt.setInto(exprParser.name());
        }

        stmt.setAlias(as());

        accept(Token.USING);

        SQLTableSource using = this.createSQLSelectParser().parseTableSource();
        stmt.setUsing(using);

        accept(Token.ON);
        stmt.setOn(exprParser.expr());

        boolean insertFlag = false;
        if (lexer.token() == Token.WHEN) {
            lexer.nextToken();
            if (lexer.token() == Token.MATCHED) {
                TeradataMergeStatement.MergeUpdateClause updateClause = new TeradataMergeStatement.MergeUpdateClause();
                lexer.nextToken();
                accept(Token.THEN);
                accept(Token.UPDATE);
                accept(Token.SET);

                for (; ; ) {
                    SQLUpdateSetItem item = this.exprParser.parseUpdateSetItem();

                    updateClause.addItem(item);
                    item.setParent(updateClause);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }

                    break;
                }

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    updateClause.setWhere(exprParser.expr());
                }

                if (lexer.token() == Token.DELETE) {
                    lexer.nextToken();
                    accept(Token.WHERE);
                    updateClause.setWhere(exprParser.expr());
                }

                stmt.setUpdateClause(updateClause);
            } else if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                insertFlag = true;
            }
        }

        if (!insertFlag) {
            if (lexer.token() == Token.WHEN) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.NOT) {
                lexer.nextToken();
                insertFlag = true;
            }
        }

        if (insertFlag) {
            TeradataMergeStatement.MergeInsertClause insertClause = new TeradataMergeStatement.MergeInsertClause();

            accept(Token.MATCHED);
            accept(Token.THEN);
            accept(Token.INSERT);

            if (lexer.token() == Token.LPAREN) {
                accept(Token.LPAREN);
                exprParser.exprList(insertClause.getColumns(), insertClause);
                accept(Token.RPAREN);
            }

            accept(Token.VALUES);
            accept(Token.LPAREN);
            exprParser.exprList(insertClause.getValues(), insertClause);
            accept(Token.RPAREN);

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                insertClause.setWhere(exprParser.expr());
            }

            stmt.setInsertClause(insertClause);
        }

        return stmt;
    }


    public TeradataDeleteStatement parseDeleteStatement() {
        TeradataDeleteStatement deleteStatement = new TeradataDeleteStatement();

        if (lexer.token() == Token.DELETE
                || lexer.token() == Token.DEL) {
            lexer.nextToken();

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.IDENTIFIER) {
                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                    SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                    deleteStatement.setFrom(tableSource);
                }
            } else if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());
            } else {
                throw new ParserException("syntax error");
            }

            if (identifierEquals("USING")) {
                lexer.nextToken();

                SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                deleteStatement.setUsing(tableSource);
            }
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            SQLExpr where = this.exprParser.expr();
            deleteStatement.setWhere(where);
        }

        if (lexer.token() == (Token.ORDER)) {
            SQLOrderBy orderBy = exprParser.parseOrderBy();
            deleteStatement.setOrderBy(orderBy);
        }

        if (lexer.token() == Token.ALL) {
            lexer.nextToken();
            deleteStatement.setAll(new SQLIdentifierExpr(lexer.stringVal()));
        }

        return deleteStatement;
    }

    public SQLUpdateStatement parseUpdateStatement() {
        return new TeradataUpdateParser(this.lexer).parseUpdateStatement();
    }

    public void parseStatementList(List<SQLStatement> statementList, int max) {
        for (; ; ) {
            if (max != -1) {
                if (statementList.size() >= max) {
                    return;
                }
            }

            if (lexer.token() == Token.EOF) {
                return;
            }
            if (lexer.token() == Token.END) {
                return;
            }
            if (lexer.token() == Token.ELSE) {
                return;
            }

            if (lexer.token() == (Token.SEMI)) {
                lexer.nextToken();
                continue;
            }

            if (lexer.token() == (Token.SELECT)) {
                statementList.add(parseSelect());
                continue;
                // add merge for td.
            } else if (lexer.token() == Token.MERGE) {
                statementList.add(parseMerge());
                continue;
            } else {
                super.parseStatementList(statementList, max);
            }
        }
    }

    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        if (lexer.token() == Token.SEL) {
            statementList.add(parseSelect());
            return true;
        } else if (lexer.token() == Token.DEL) {
            statementList.add(parseDeleteStatement());
            return true;
        }
        return false;
    }

    public SQLStatement parseCreateDatabase() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        accept(Token.DATABASE);

        TeradataCreateDatabaseStatement stmt = new TeradataCreateDatabaseStatement();
        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.AS) {
            lexer.nextToken();
            for (; ; ) {
                if (lexer.token() == Token.IDENTIFIER) {
                    if ("perm".equalsIgnoreCase(lexer.stringVal())) {
                        stmt.setPerm(this.exprParser.expr());
                        lexer.nextToken();
                        continue;
                    }
                    if ("spool".equalsIgnoreCase(lexer.stringVal())) {
                        stmt.setSpool(this.exprParser.expr());
                        lexer.nextToken();
                        continue;
                    }
                }

//                if (lexer.token() == Token.SEMI) {
//                    lexer.nextToken();
//                    continue;
//                }
//                if (lexer.token() == Token.EOF) {
//                    break;
//                }
                break;
            }
        }
        return stmt;
    }

}
