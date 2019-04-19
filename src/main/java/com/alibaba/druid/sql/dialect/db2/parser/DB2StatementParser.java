/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.sql.dialect.db2.parser;

import java.util.List;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2CreateTableStatement;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2ValuesStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLCreateTableParser;
import com.alibaba.druid.sql.parser.SQLParserFeature;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;


public class DB2StatementParser extends SQLStatementParser {
    //need to specify
    private static final String AUTO_INCREMENT = "AUTO_INCREMENT";
    private static final String COLLATE2 = "COLLATE";
    private static final String CHAIN = "CHAIN";
    private static final String ENGINES = "ENGINES";
    private static final String ENGINE = "ENGINE";
    private static final String BINLOG = "BINLOG";
    private static final String EVENTS = "EVENTS";
    private static final String SESSION = "SESSION";
    private static final String GLOBAL = "GLOBAL";
    private static final String VARIABLES = "VARIABLES";
    private static final String STATUS = "STATUS";
    private static final String RESET = "RESET";
    private static final String DESCRIBE = "DESCRIBE";
    private static final String WRITE = "WRITE";
    private static final String READ = "READ";
    private static final String LOCAL = "LOCAL";
    private static final String TABLES = "TABLES";
    private static final String TEMPORARY = "TEMPORARY";
    private static final String SPATIAL = "SPATIAL";
    private static final String LOW_PRIORITY = "LOW_PRIORITY";
    private static final String CONNECTION = "CONNECTION";
    private static final String EXTENDED = "EXTENDED";
    private static final String PARTITIONS = "PARTITIONS";
    private static final String FORMAT = "FORMAT";

    private int maxIntoClause = -1;


    public DB2StatementParser(String sql) {
        super(new DB2ExprParser(sql));
    }

    public DB2StatementParser(String sql, SQLParserFeature... features) {
        super(new DB2ExprParser(sql, features));
    }

    public DB2StatementParser(Lexer lexer) {
        super(new DB2ExprParser(lexer));
    }

    public DB2SelectParser createSQLSelectParser() {
        return new DB2SelectParser(this.exprParser, selectListCache);
    }

    //TODO need to enhance
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

        if (lexer.token() == Token.TABLE || lexer.identifierEquals(TEMPORARY)) {
            //TODO refactor DB2CreateTableParser
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            DB2CreateTableParser parser = new DB2CreateTableParser(this.exprParser);
            DB2CreateTableStatement stmt = (DB2CreateTableStatement) parser.parseCreateTable(false);
            if (comments != null) {
                stmt.addBeforeComment(comments);
            }
            return stmt;
        }

        if (lexer.token() == Token.DATABASE || lexer.token() == Token.SCHEMA) {
            if (replace) {
                lexer.reset(markBp, markChar, Token.CREATE);
            }
            return parseCreateDatabase();
        }

        if (lexer.token() == Token.UNIQUE || lexer.token() == Token.INDEX || lexer.token() == Token.FULLTEXT
                || lexer.identifierEquals(SPATIAL)) {
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

    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        //TODO 此处是否加入对LIKE的判断
        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();
            DB2ValuesStatement stmt = new DB2ValuesStatement();
            stmt.setExpr(this.exprParser.expr());
            statementList.add(stmt);
            return true;
        }

        return false;
    }


    public SQLCreateTableParser getSQLCreateTableParser() {
        return new DB2CreateTableParser(this.exprParser);
    }

    protected SQLAlterTableAlterColumn parseAlterColumn() {
        if (lexer.token() == Token.COLUMN) {
            lexer.nextToken();
        }

        SQLColumnDefinition column = this.exprParser.parseColumn();

        SQLAlterTableAlterColumn alterColumn = new SQLAlterTableAlterColumn();
        alterColumn.setColumn(column);

        if (column.getDataType() == null && column.getConstraints().size() == 0) {
            if (lexer.token() == Token.SET) {
                lexer.nextToken();
                if (lexer.token() == Token.NOT) {
                    lexer.nextToken();
                    accept(Token.NULL);
                    alterColumn.setSetNotNull(true);
                } else if (lexer.token() == Token.DEFAULT) {
                    lexer.nextToken();
                    SQLExpr defaultValue = this.exprParser.expr();
                    alterColumn.setSetDefault(defaultValue);
                } else if (lexer.identifierEquals(FnvHash.Constants.DATA)) {
                    lexer.nextToken();
                    acceptIdentifier("TYPE");
                    SQLDataType dataType = this.exprParser.parseDataType();
                    alterColumn.setDataType(dataType);
                } else {
                    throw new ParserException("TODO : " + lexer.info());
                }
            } else if (lexer.token() == Token.DROP) {
                lexer.nextToken();
                if (lexer.token() == Token.NOT) {
                    lexer.nextToken();
                    accept(Token.NULL);
                    alterColumn.setDropNotNull(true);
                } else {
                    accept(Token.DEFAULT);
                    alterColumn.setDropDefault(true);
                }
            }
        }
        return alterColumn;
    }
}
