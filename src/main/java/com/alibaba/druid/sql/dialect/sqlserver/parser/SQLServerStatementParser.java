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
package com.alibaba.druid.sql.dialect.sqlserver.parser;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerOutput;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.*;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerExecStatement.SQLServerParameter;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.log4j.net.SMTPAppender;
import org.codehaus.janino.IClass;
import org.hibernate.boot.jaxb.hbm.spi.PluralAttributeInfoPrimitiveArrayAdapter;

import java.util.Collection;
import java.util.List;

public class SQLServerStatementParser extends SQLStatementParser {

    public SQLServerStatementParser(String sql) {
        super(new SQLServerExprParser(sql));
    }

    public SQLServerStatementParser(String sql, SQLParserFeature... features) {
        super(new SQLServerExprParser(sql, features));
    }

    public SQLSelectParser createSQLSelectParser() {
        return new SQLServerSelectParser(this.exprParser, selectListCache);
    }

    public SQLServerStatementParser(Lexer lexer) {
        super(new SQLServerExprParser(lexer));
    }

    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        if (lexer.token() == Token.WITH) {
            SQLStatement stmt = parseSelect();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.EXEC) || lexer.identifierEquals(FnvHash.Constants.EXECUTE)) {
            lexer.nextToken();

            SQLServerExecStatement execStmt = new SQLServerExecStatement();
            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                this.parseExecParameter(execStmt.getParameters(), execStmt);
                accept(Token.RPAREN);
            } else {
                SQLName sqlNameName = this.exprParser.name();

                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                    execStmt.setReturnStatus(sqlNameName);
                    execStmt.setModuleName(this.exprParser.name());
                } else {
                    execStmt.setModuleName(sqlNameName);
                }

                this.parseExecParameter(execStmt.getParameters(), execStmt);
            }
            statementList.add(execStmt);
            return true;
        }

        if (lexer.token() == Token.DECLARE) {
            statementList.add(this.parseDeclare());
            return true;
        }

        if (lexer.token() == Token.IF) {
            statementList.add(this.parseIf());
            return true;
        }

        if (lexer.token() == Token.BEGIN) {
            statementList.add(this.parseBlock());
            return true;
        }

        if (lexer.token() == Token.COMMIT) {
            statementList.add(this.parseCommit());
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.WAITFOR)) {
            statementList.add(this.parseWaitFor());
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.GO)) {
            lexer.nextToken();

            SQLStatement stmt = new SQLScriptCommitStatement();
            statementList.add(stmt);
            return true;
        }

        return false;
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
        Token token = lexer.token();
        if (token == Token.TABLE || lexer.identifierEquals("GLOBAL")) {
            SQLCreateTableParser createTableParser = getSQLCreateTableParser();
            //CREATE后为TABLE即可视为建表语句 -- 在特定的数据库实现中还可以对具体如LIKE来限制
            SQLCreateTableStatement stmt = createTableParser.parseCreateTable(false);

            if (comments != null) {
                stmt.addBeforeComment(comments);
            }

            return stmt;
        } else if (token == Token.INDEX //
                || token == Token.UNIQUE //
                || lexer.identifierEquals("NONCLUSTERED") // sql server
        ) {
            return parseCreateIndex(false);
        } else if (lexer.token() == Token.SEQUENCE) {
            return parseCreateSequence(false);
        } else if (token == Token.OR) {
            lexer.nextToken();
            accept(Token.REPLACE);

            if (lexer.identifierEquals("FORCE")) {
                lexer.nextToken();
            }
            if (lexer.token() == Token.PROCEDURE) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateProcedure();
            }

            if (lexer.token() == Token.VIEW) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateView();
            }

            if (lexer.token() == Token.TRIGGER) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateTrigger();
            }

            if (lexer.token() == Token.FUNCTION) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateFunction();
            }

            if (lexer.identifierEquals(FnvHash.Constants.PACKAGE)) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreatePackage();
            }

            if (lexer.identifierEquals(FnvHash.Constants.TYPE)) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateType();
            }

            if (lexer.identifierEquals(FnvHash.Constants.PUBLIC)) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateSynonym();
            }

            if (lexer.identifierEquals(FnvHash.Constants.SYNONYM)) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateSynonym();
            }

            // lexer.reset(mark_bp, mark_ch, Token.CREATE);
            throw new ParserException("TODO " + lexer.info());
        } else if (token == Token.DATABASE) {
            lexer.nextToken();
            if (lexer.identifierEquals("LINK")) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateDbLink();
            }

            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateDatabase();
        } else if (lexer.token() == Token.USER) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateUser();
        } else if (lexer.token() == Token.LOGIN) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateLogin();
        } else if (lexer.identifierEquals(FnvHash.Constants.PUBLIC)) {
            lexer.nextToken();
            if (lexer.identifierEquals("SYNONYM")) {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateSynonym();
            } else {
                lexer.reset(markBp, markChar, Token.CREATE);
                return parseCreateDbLink();
            }
        } else if (lexer.identifierEquals("SHARE")) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateDbLink();
        } else if (lexer.identifierEquals("SYNONYM")) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateSynonym();
        } else if (token == Token.VIEW) {
            return parseCreateView();
        } else if (token == Token.TRIGGER) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateTrigger();
        } else if (token == Token.PROCEDURE) {
            lexer.reset(markBp, markChar, Token.CREATE);
            SQLCreateProcedureStatement stmt = parseCreateProcedure();
            stmt.setCreate(true);
            return stmt;
        } else if (token == Token.FUNCTION) {
            lexer.reset(markBp, markChar, Token.CREATE);
            SQLStatement stmt = this.parseCreateFunction();
            return stmt;
        } else if (lexer.identifierEquals(FnvHash.Constants.BITMAP)) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateIndex(true);
        } else if (lexer.identifierEquals(FnvHash.Constants.MATERIALIZED)) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateMaterializedView();
        } else if (lexer.identifierEquals(FnvHash.Constants.TYPE)) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateType();
        } else if (token == Token.SCHEMA) {
            lexer.reset(markBp, markChar, Token.CREATE);
            return parseCreateSchema();
        }

        throw new ParserException("TODO " + lexer.token());
    }

    /**
     * SQLServer parse Parameter stmt support out type
     *
     * @author zz [455910092@qq.com]
     */
    public void parseExecParameter(Collection<SQLServerParameter> exprCol, SQLObject parent) {
        if (lexer.token() == Token.RPAREN || lexer.token() == Token.RBRACKET) {
            return;
        }

        if (lexer.token() == Token.EOF) {
            return;
        }
        SQLServerParameter param = new SQLServerParameter();
        SQLExpr expr = this.exprParser.expr();
        expr.setParent(parent);
        param.setExpr(expr);
        if (lexer.token() == Token.OUT) {
            param.setType(true);
            accept(Token.OUT);
        }
        exprCol.add(param);
        while (lexer.token() == Token.COMMA) {
            lexer.nextToken();
            param = new SQLServerParameter();
            expr = this.exprParser.expr();
            expr.setParent(parent);
            param.setExpr(expr);
            if (lexer.token() == Token.OUT) {
                param.setType(true);
                accept(Token.OUT);
            }
            exprCol.add(param);
        }
    }

    public SQLStatement parseDeclare() {
        this.accept(Token.DECLARE);

        SQLDeclareStatement declareStatement = new SQLDeclareStatement();

        for (; ; ) {
            SQLDeclareItem item = new SQLDeclareItem();
            declareStatement.addItem(item);

            item.setName(this.exprParser.name());

            if (lexer.token() == Token.AS) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.TABLE) {
                lexer.nextToken();
                item.setType(SQLDeclareItem.Type.TABLE);

                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();

                    for (; ; ) {
                        if (lexer.token() == Token.IDENTIFIER //
                                || lexer.token() == Token.LITERAL_ALIAS) {
                            SQLColumnDefinition column = this.exprParser.parseColumn();
                            item.getTableElementList().add(column);
                        } else if (lexer.token() == Token.PRIMARY //
                                || lexer.token() == Token.UNIQUE //
                                || lexer.token() == Token.CHECK //
                                || lexer.token() == Token.CONSTRAINT) {
                            SQLConstraint constraint = this.exprParser.parseConstaint();
                            constraint.setParent(item);
                            item.getTableElementList().add((SQLTableElement) constraint);
                        } else if (lexer.token() == Token.TABLESPACE) {
                            throw new ParserException("TODO " + lexer.info());
                        } else {
                            SQLColumnDefinition column = this.exprParser.parseColumn();
                            item.getTableElementList().add(column);
                        }

                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            }
                            continue;
                        }

                        break;
                    }
                    accept(Token.RPAREN);
                }
                break;
            } else if (lexer.token() == Token.CURSOR) {
                item.setType(SQLDeclareItem.Type.CURSOR);
                lexer.nextToken();
            } else {
                item.setType(SQLDeclareItem.Type.LOCAL);
                item.setDataType(this.exprParser.parseDataType());
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                    item.setValue(this.exprParser.expr());
                }
            }

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }
        return declareStatement;
    }

    public SQLStatement parseInsert() {
        SQLServerInsertStatement insertStatement = new SQLServerInsertStatement();

        if (lexer.token() == Token.INSERT) {
            accept(Token.INSERT);
        }

        parseInsert0(insertStatement);
        return insertStatement;
    }

    protected void parseInsert0(SQLInsertInto insert, boolean acceptSubQuery) {
        SQLServerInsertStatement insertStatement = (SQLServerInsertStatement) insert;

        SQLServerTop top = this.getExprParser().parseTop();
        if (top != null) {
            insertStatement.setTop(top);
        }

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        }

        SQLName tableName = this.exprParser.name();
        insertStatement.setTableName(tableName);

        if (lexer.token() == Token.LITERAL_ALIAS) {
            insertStatement.setAlias(tableAlias());
        }

        parseInsert0_hinits(insertStatement);

        if (lexer.token() == Token.IDENTIFIER && !lexer.stringVal().equalsIgnoreCase("OUTPUT")) {
            insertStatement.setAlias(lexer.stringVal());
            lexer.nextToken();
        }

        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            this.exprParser.exprList(insertStatement.getColumns(), insertStatement);
            accept(Token.RPAREN);
        }

        SQLServerOutput output = this.getExprParser().parserOutput();
        if (output != null) {
            insertStatement.setOutput(output);
        }

        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();

            for (; ; ) {
                accept(Token.LPAREN);
                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
                this.exprParser.exprList(values.getValues(), values);
                insertStatement.addValueCause(values);
                accept(Token.RPAREN);

                if (!parseCompleteValues && insertStatement.getValuesList().size() >= parseValuesSize) {
                    lexer.skipToEOF();
                    break;
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
        } else if (acceptSubQuery && (lexer.token() == Token.SELECT || lexer.token() == Token.LPAREN)) {
            SQLQueryExpr queryExpr = (SQLQueryExpr) this.exprParser.expr();
            insertStatement.setQuery(queryExpr.getSubQuery());
        } else if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
            accept(Token.VALUES);
            insertStatement.setDefaultValues(true);
        }
    }

    protected SQLServerUpdateStatement createUpdateStatement() {
        return new SQLServerUpdateStatement();
    }

    public SQLUpdateStatement parseUpdateStatement() {
        SQLServerUpdateStatement udpateStatement = createUpdateStatement();

        accept(Token.UPDATE);

        SQLServerTop top = this.getExprParser().parseTop();
        if (top != null) {
            udpateStatement.setTop(top);
        }

        SQLTableSource tableSource = this.exprParser.createSelectParser().parseTableSource();
        udpateStatement.setTableSource(tableSource);

        parseUpdateSet(udpateStatement);

        SQLServerOutput output = this.getExprParser().parserOutput();
        if (output != null) {
            udpateStatement.setOutput(output);
        }

        if (lexer.token() == Token.FROM) {
            lexer.nextToken();
            SQLTableSource from = this.exprParser.createSelectParser().parseTableSource();
            udpateStatement.setFrom(from);
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            udpateStatement.setWhere(this.exprParser.expr());
        }

        return udpateStatement;
    }

    public SQLServerExprParser getExprParser() {
        return (SQLServerExprParser) exprParser;
    }

    public SQLStatement parseSet() {
        accept(Token.SET);

        if (lexer.identifierEquals(FnvHash.Constants.TRANSACTION)) {
            lexer.nextToken();
            acceptIdentifier("ISOLATION");
            acceptIdentifier("LEVEL");

            SQLServerSetTransactionIsolationLevelStatement stmt = new SQLServerSetTransactionIsolationLevelStatement();

            if (lexer.identifierEquals("READ")) {
                lexer.nextToken();

                if (lexer.identifierEquals("UNCOMMITTED")) {
                    stmt.setLevel("READ UNCOMMITTED");
                    lexer.nextToken();
                } else if (lexer.identifierEquals("COMMITTED")) {
                    stmt.setLevel("READ COMMITTED");
                    lexer.nextToken();
                } else {
                    throw new ParserException("UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
                }
            } else if (lexer.identifierEquals("SERIALIZABLE")) {
                stmt.setLevel("SERIALIZABLE");
                lexer.nextToken();
            } else if (lexer.identifierEquals("SNAPSHOT")) {
                stmt.setLevel("SNAPSHOT");
                lexer.nextToken();
            } else if (lexer.identifierEquals("REPEATABLE")) {
                lexer.nextToken();
                if (lexer.identifierEquals("READ")) {
                    stmt.setLevel("REPEATABLE READ");
                    lexer.nextToken();
                } else {
                    throw new ParserException("UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
                }
            } else {
                throw new ParserException("UNKOWN TRANSACTION LEVEL : " + lexer.stringVal() + ", " + lexer.info());
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.STATISTICS)) {
            lexer.nextToken();

            SQLSetStatement stmt = new SQLSetStatement();

            if (lexer.identifierEquals("IO") || lexer.identifierEquals("XML") || lexer.identifierEquals("PROFILE")
                    || lexer.identifierEquals("TIME")) {

                SQLExpr target = new SQLIdentifierExpr("STATISTICS " + lexer.stringVal().toUpperCase());

                lexer.nextToken();
                if (lexer.token() == Token.ON) {
                    stmt.set(target, new SQLIdentifierExpr("ON"));
                    lexer.nextToken();
                } else if (lexer.identifierEquals(FnvHash.Constants.OFF)) {
                    stmt.set(target, new SQLIdentifierExpr("OFF"));
                    lexer.nextToken();
                }
            }
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.IDENTITY_INSERT)) {
            SQLSetStatement stmt = new SQLSetStatement();
            stmt.setOption(SQLSetStatement.Option.IDENTITY_INSERT);

            lexer.nextToken();
            SQLName table = this.exprParser.name();

            if (lexer.token() == Token.ON) {
                SQLExpr value = new SQLIdentifierExpr("ON");
                stmt.set(table, value);
                lexer.nextToken();
            } else if (lexer.identifierEquals(FnvHash.Constants.OFF)) {
                SQLExpr value = new SQLIdentifierExpr("OFF");
                stmt.set(table, value);
                lexer.nextToken();
            }
            return stmt;
        }

        if (lexer.token() == Token.VARIANT) {
            SQLSetStatement stmt = new SQLSetStatement(getDbType());
            parseAssignItems(stmt.getItems(), stmt);
            return stmt;
        } else {
            SQLSetStatement stmt = new SQLSetStatement();
            SQLExpr target = this.exprParser.expr();

            if (lexer.token() == Token.ON) {
                stmt.set(target, new SQLIdentifierExpr("ON"));
                lexer.nextToken();
            } else if (lexer.identifierEquals("OFF")) {
                stmt.set(target, new SQLIdentifierExpr("OFF"));
                lexer.nextToken();
            } else {
                stmt.set(target, this.exprParser.expr());
            }
            return stmt;
        }
    }

    public SQLIfStatement parseIf() {
        accept(Token.IF);

        SQLIfStatement stmt = new SQLIfStatement();

        stmt.setCondition(this.exprParser.expr());

        this.parseStatementList(stmt.getStatements(), 1, stmt);

        if (lexer.token() == Token.SEMI) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.ELSE) {
            lexer.nextToken();

            SQLIfStatement.Else elseItem = new SQLIfStatement.Else();
            this.parseStatementList(elseItem.getStatements(), 1, elseItem);
            stmt.setElseItem(elseItem);
        }

        return stmt;
    }

    public SQLStatement parseBlock() {
        accept(Token.BEGIN);

        if (lexer.identifierEquals("TRANSACTION") || lexer.identifierEquals("TRAN")) {
            lexer.nextToken();

            SQLStartTransactionStatement startTrans = new SQLStartTransactionStatement();
            startTrans.setDbType(dbType);

            if (lexer.token() == Token.IDENTIFIER) {
                SQLName name = this.exprParser.name();
                startTrans.setName(name);
            }
            return startTrans;
        }

        SQLBlockStatement block = new SQLBlockStatement();
        parseStatementList(block.getStatementList());

        accept(Token.END);

        return block;
    }

    public SQLStatement parseCommit() {
        acceptIdentifier("COMMIT");

        SQLCommitStatement stmt = new SQLCommitStatement();

        if (lexer.identifierEquals("WORK")) {
            lexer.nextToken();
            stmt.setWork(true);
        }

        if (lexer.identifierEquals("TRAN") || lexer.identifierEquals("TRANSACTION")) {
            lexer.nextToken();
            if (lexer.token() == Token.IDENTIFIER || lexer.token() == Token.VARIANT) {
                stmt.setTransactionName(this.exprParser.expr());
            }

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                accept(Token.LPAREN);
                acceptIdentifier("DELAYED_DURABILITY");
                accept(Token.EQ);
                stmt.setDelayedDurability(this.exprParser.expr());
                accept(Token.RPAREN);
            }

        }

        return stmt;
    }

    public SQLServerRollbackStatement parseRollback() {
        acceptIdentifier("ROLLBACK");

        SQLServerRollbackStatement stmt = new SQLServerRollbackStatement();

        if (lexer.identifierEquals("WORK")) {
            lexer.nextToken();
            stmt.setWork(true);
        }

        if (lexer.identifierEquals("TRAN") || lexer.identifierEquals("TRANSACTION")) {
            lexer.nextToken();
            if (lexer.token() == Token.IDENTIFIER || lexer.token() == Token.VARIANT) {
                stmt.setName(this.exprParser.expr());
            }


        }

        return stmt;
    }

    public SQLServerWaitForStatement parseWaitFor() {
        acceptIdentifier("WAITFOR");

        SQLServerWaitForStatement stmt = new SQLServerWaitForStatement();

        if (lexer.identifierEquals("DELAY")) {
            lexer.nextToken();
            stmt.setDelay(this.exprParser.expr());
        }

        if (lexer.identifierEquals("TIME")) {
            lexer.nextToken();
            stmt.setTime(this.exprParser.expr());
        }

        if (lexer.token() == Token.COMMA) {
            lexer.nextToken();
            if (lexer.identifierEquals("TIMEOUT")) {
                lexer.nextToken();
                stmt.setTimeout(this.exprParser.expr());
            }
        }

        return stmt;
    }

    public SQLStatement parseCreateUser() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        accept(Token.USER);

        SQLServerCreateUserStatement stmt = new SQLServerCreateUserStatement();

        SQLExpr expr = exprParser.primary();
        stmt.setUser(expr);

        if (lexer.token() == Token.FOR) {
            lexer.nextToken();
            if (lexer.token() == Token.LOGIN) {
                lexer.nextToken();
                if (lexer.token() == Token.IDENTIFIER) {
                    SQLExpr login = this.exprParser.expr();
                    stmt.setLogin(login);
                }

            }
        }

        return stmt;

    }

    public SQLStatement parseCreateSchema() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }

        accept(Token.SCHEMA);

        SQLServerCreateSchemaStatement stmt = new SQLServerCreateSchemaStatement();

        SQLExpr expr = exprParser.primary();
        stmt.setSchema(expr);

        return stmt;

    }


    public SQLStatement parseCreateLogin() {
        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
        }
        accept(Token.LOGIN);
        SQLServerCreateLoginStatement stmt = new SQLServerCreateLoginStatement();
        // TODO

        SQLExpr expr = exprParser.primary();
        stmt.setLogin(expr);

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            if (lexer.identifierEquals("PASSWORD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
//                userSpec.setAuthPlugin(this.exprParser.expr());
                }
            }

            SQLExpr password = this.exprParser.expr();
            stmt.setPassword(password);
        }
        return stmt;

    }

    public SQLStatement parseDrop() {
        List<String> beforeComments = null;
        if (lexer.isKeepComments() && lexer.hasComment()) {
            beforeComments = lexer.readAndResetComments();
        }

        lexer.nextToken();

        final SQLStatement stmt;

        List<SQLCommentHint> hints = null;
        if (lexer.token() == Token.HINT) {
            hints = this.exprParser.parseHints();
        }

        if (lexer.token() == Token.TABLE || lexer.identifierEquals("TEMPORARY")) {
            SQLDropTableStatement dropTable = parseDropTable(false);
            if (hints != null) {
                dropTable.setHints(hints);
            }
            stmt = dropTable;
        } else if (lexer.token() == Token.USER) {
            stmt = parseDropUser();
        } else if (lexer.token() == Token.LOGIN) {
            stmt = parseDropLogin();
        } else if (lexer.token() == Token.INDEX) {
            stmt = parseDropIndex();
        } else if (lexer.token() == Token.VIEW) {
            stmt = parseDropView(false);
        } else if (lexer.token() == Token.TRIGGER) {
            stmt = parseDropTrigger(false);
        } else if (lexer.token() == Token.DATABASE || lexer.token() == Token.SCHEMA) {
            stmt = parseDropDatabase(false);
        } else if (lexer.token() == Token.FUNCTION) {
            stmt = parseDropFunction(false);
        } else if (lexer.token() == Token.TABLESPACE) {
            stmt = parseDropTablespace(false);

        } else if (lexer.token() == Token.PROCEDURE) {
            stmt = parseDropProcedure(false);

        } else if (lexer.token() == Token.SEQUENCE) {
            stmt = parseDropSequence(false);

        } else if (lexer.identifierEquals(FnvHash.Constants.EVENT)) {
            stmt = parseDropEvent();

        } else if (lexer.identifierEquals(FnvHash.Constants.LOGFILE)) {
            stmt = parseDropLogFileGroup();

        } else if (lexer.identifierEquals(FnvHash.Constants.SERVER)) {
            stmt = parseDropServer();

        } else {
            throw new ParserException("TODO " + lexer.info());
        }

        if (beforeComments != null) {
            stmt.addBeforeComment(beforeComments);
        }
        return stmt;
    }

    public SQLStatement parseDropLogin() {
        accept(Token.LOGIN);

        SQLServerDropLoginStatement stmt = new SQLServerDropLoginStatement();
//        SQLServerDropLoginStatement stmt = new SQLServerDropLoginStatement(getDbType());
        for (; ; ) {
            SQLExpr expr = this.exprParser.expr();
            stmt.addLogin(expr);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        return stmt;


//        SQLDropServerStatement stmt = new SQLDropServerStatement();
//        stmt.setDbType(dbType);
//
//        if (lexer.token() == Token.IF) {
//            lexer.nextToken();
//            accept(Token.EXISTS);
//            stmt.setIfExists(true);
//        }
//
//        SQLName name = this.exprParser.name();
//        stmt.setName(name);
//
//        return stmt;
    }

    /**
     * parse create procedure stmt
     */
    public SQLCreateProcedureStatement parseCreateProcedure() {
        SQLCreateProcedureStatement stmt = new SQLCreateProcedureStatement();
        stmt.setDbType(dbType);

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();
            if (lexer.token() == Token.OR) {
                lexer.nextToken();
                accept(Token.REPLACE);
                stmt.setOrReplace(true);
            }
        } else {
            stmt.setCreate(false);
        }

        accept(Token.PROCEDURE);

        SQLName procedureName = this.exprParser.name();
        stmt.setName(procedureName);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
            accept(Token.RPAREN);
        } else if (lexer.token() != Token.AS) {
//            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
        }

        if (lexer.identifierEquals("AUTHID")) {
            lexer.nextToken();
            String strVal = lexer.stringVal();
            if (lexer.identifierEquals("CURRENT_USER")) {
                lexer.nextToken();
            } else {
                acceptIdentifier("DEFINER");
            }
            SQLName authid = new SQLIdentifierExpr(strVal);
            stmt.setAuthid(authid);
        }

        if (lexer.identifierEquals(FnvHash.Constants.WRAPPED)) {
            lexer.nextToken();
            int pos = lexer.text.indexOf(';', lexer.pos());
            if (pos != -1) {
                String wrappedString = lexer.subString(lexer.pos(), pos - lexer.pos());
                stmt.setWrappedSource(wrappedString);
                lexer.reset(pos, ';', Token.LITERAL_CHARS);
                lexer.nextToken();
                stmt.setAfterSemi(true);
            } else {
                String wrappedString = lexer.text.substring(lexer.pos());
                stmt.setWrappedSource(wrappedString);
                lexer.reset(lexer.text.length(), (char) LayoutCharacters.EOI, Token.EOF);
            }
            return stmt;
        }

        if (lexer.token() == Token.SEMI) {
            lexer.nextToken();
            return stmt;
        }

        if (lexer.token() == Token.AS) {
            accept(Token.AS);
        }

        SQLStatement block = this.parseBlock();

        stmt.setBlock(block);

        if (lexer.identifierEquals(procedureName.getSimpleName())) {
            lexer.nextToken();
        }

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
            } else if (lexer.token() != Token.AS) {
                parameter.setParamType(SQLParameter.ParameterType.DEFAULT);// default parameter type is in
                parameter.setName(this.exprParser.name());
                parameter.setDataType(this.exprParser.parseDataType());

                if (lexer.token() == Token.COLONEQ) {
                    lexer.nextToken();
                    parameter.setDefaultValue(this.exprParser.expr());
                }
                parameters.add(parameter);
            }
            if (lexer.token() == Token.COMMA || lexer.token() == Token.SEMI) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.AS) {
                break;
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

    public SQLCreateFunctionStatement parseCreateFunction() {
        SQLCreateFunctionStatement stmt = new SQLCreateFunctionStatement();
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
            SQLName definer = this.getExprParser().name();
            stmt.setDefiner(definer);
        }

        accept(Token.FUNCTION);

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LPAREN) {// match "("
            lexer.nextToken();
            parserParameters(stmt.getParameters(), stmt);
            accept(Token.RPAREN);// match ")"
        }

        acceptIdentifier("RETURNS");
        SQLDataType dataType = this.exprParser.parseDataType();
        stmt.setReturnDataType(dataType);

        for (;;) {
            if (lexer.identifierEquals("DETERMINISTIC")) {
                lexer.nextToken();
                stmt.setDeterministic(true);
                continue;
            }

            break;
        }

        SQLStatement block;
        if (lexer.token() == Token.AS) {
            lexer.nextToken();
        }
        if (lexer.token() == Token.RETURN) {
            block = this.parseBlock();
        }
        if (lexer.token() == Token.BEGIN) {
            block = this.parseBlock();
        } else {
            block = this.parseStatement();
        }

        stmt.setBlock(block);

        return stmt;
    }
}
