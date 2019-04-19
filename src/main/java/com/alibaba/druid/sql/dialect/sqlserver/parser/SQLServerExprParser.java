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

import java.util.Arrays;
import java.util.List;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerOutput;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.dialect.sqlserver.ast.expr.SQLServerObjectReferenceExpr;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;

public class SQLServerExprParser extends SQLExprParser {
    public final static String[] AGGREGATE_FUNCTIONS;

    public final static long[] AGGREGATE_FUNCTIONS_CODES;

    static {
        String[] strings = {
                "AVG",
                "COUNT",
                "FIRST_VALUE",
                "MAX",
                "MIN",
                "ROW_NUMBER",
                "STDDEV",
                "SUM"
        };
        AGGREGATE_FUNCTIONS_CODES = FnvHash.fnv1a_64_lower(strings, true);
        AGGREGATE_FUNCTIONS = new String[AGGREGATE_FUNCTIONS_CODES.length];
        for (String str : strings) {
            long hash = FnvHash.fnv1a_64_lower(str);
            int index = Arrays.binarySearch(AGGREGATE_FUNCTIONS_CODES, hash);
            AGGREGATE_FUNCTIONS[index] = str;
        }
    }

    public SQLServerExprParser(Lexer lexer){
        super(lexer);
        this.dbType = JdbcConstants.SQL_SERVER;
        this.aggregateFunctions = AGGREGATE_FUNCTIONS;
        this.aggregateFunctionHashCodes = AGGREGATE_FUNCTIONS_CODES;
    }

    public SQLServerExprParser(String sql){
        this(new SQLServerLexer(sql));
        this.lexer.nextToken();
        this.dbType = JdbcConstants.SQL_SERVER;
    }

    public SQLServerExprParser(String sql, SQLParserFeature... features){
        this(new SQLServerLexer(sql, features));
        this.lexer.nextToken();
        this.dbType = JdbcConstants.SQL_SERVER;
    }

    public SQLDataType parseDataType() {
        return parseDataType(true);
    }

    // TODO returns为类型，会与TABLE冲突，需要解决
    public SQLDataType parseDataType(boolean restrict) {
        Token token = lexer.token();
        if (token == Token.DEFAULT || token == Token.NOT || token == Token.NULL) {
            return null;
        }

        SQLName typeExpr = name();
        final long typeNameHashCode = typeExpr.nameHashCode64();
        String typeName = typeExpr.toString();

        if (typeNameHashCode == FnvHash.Constants.LONG
                && lexer.identifierEquals(FnvHash.Constants.BYTE)
                && JdbcConstants.MYSQL.equals(getDbType()) //
        ) {
            typeName += (' ' + lexer.stringVal());
            lexer.nextToken();
        } else if (typeNameHashCode == FnvHash.Constants.DOUBLE
                && JdbcConstants.POSTGRESQL.equals(getDbType()) //
        ) {
            typeName += (' ' + lexer.stringVal());
            lexer.nextToken();
        }

        if (typeNameHashCode == FnvHash.Constants.UNSIGNED) {
            if (lexer.token() == Token.IDENTIFIER) {
                typeName += (' ' + lexer.stringVal());
                lexer.nextToken();
            }
        }

        if (isCharType(typeName)) {
            SQLCharacterDataType charType = new SQLCharacterDataType(typeName);

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                SQLExpr arg = this.expr();
                arg.setParent(charType);
                charType.addArgument(arg);
                accept(Token.RPAREN);
            }

            charType = (SQLCharacterDataType) parseCharTypeRest(charType);

            if (lexer.token() == Token.HINT) {
                List<SQLCommentHint> hints = this.parseHints();
                charType.setHints(hints);
            }

            return charType;
        }

        if ("character".equalsIgnoreCase(typeName) && "varying".equalsIgnoreCase(lexer.stringVal())) {
            typeName += ' ' + lexer.stringVal();
            lexer.nextToken();
        }

        SQLDataType dataType = new SQLDataTypeImpl(typeName);
        dataType.setDbType(dbType);

        return parseDataTypeRest(dataType);
    }

    // 获取SQLName的name
    public SQLName name() {
        String identName;
        long hash = 0;
        if (lexer.token() == Token.LITERAL_ALIAS) {
            identName = lexer.stringVal();
            lexer.nextToken();
        } else if (lexer.token() == Token.IDENTIFIER) {
            identName = lexer.stringVal();

            char c0 = identName.charAt(0);
            if (c0 != '[') {
                hash = lexer.hash_lower();
            }
            lexer.nextToken();
        } else if (lexer.token() == Token.LITERAL_CHARS) {
            identName = '\'' + lexer.stringVal() + '\'';
            lexer.nextToken();
        } else if (lexer.token() == Token.VARIANT) {
            identName = lexer.stringVal();
            lexer.nextToken();
        } else if (lexer.token() == Token.WITH) {
            identName = lexer.stringVal();
            lexer.nextToken();
        } else {
            switch (lexer.token()) {
                case MODEL:
                case PCTFREE:
                case INITRANS:
                case MAXTRANS:
                case SEGMENT:
                case CREATION:
                case IMMEDIATE:
                case DEFERRED:
                case STORAGE:
                case NEXT:
                case MINEXTENTS:
                case MAXEXTENTS:
                case MAXSIZE:
                case PCTINCREASE:
                case FLASH_CACHE:
                case CELL_FLASH_CACHE:
                case NONE:
                case LOB:
                case STORE:
                case ROW:
                case CHUNK:
                case CACHE:
                case NOCACHE:
                case LOGGING:
                case NOCOMPRESS:
                case KEEP_DUPLICATES:
                case EXCEPTIONS:
                case PURGE:
                case INITIALLY:
                case END:
                case COMMENT:
                case ENABLE:
                case DISABLE:
                case SEQUENCE:
                case USER:
                case ANALYZE:
                case OPTIMIZE:
                case GRANT:
                case REVOKE:
                    // binary有很多含义，lexer识别了这个token，实际上应该当做普通IDENTIFIER
                case BINARY:
                case OVER:
                case ORDER:
                case DO:
                case JOIN:
                case TYPE:
                case FUNCTION:
                case KEY:
                case SCHEMA:
                case TABLE:
                    identName = lexer.stringVal();
                    lexer.nextToken();
                    break;
                case INTERVAL:
                case EXPLAIN:
                case PARTITION:
                case SET:
                case DEFAULT:
                    identName = lexer.stringVal();
                    lexer.nextToken();
                    break;
                default:
                    throw new ParserException("error " + lexer.info());
            }
        }

        SQLName name = new SQLIdentifierExpr(identName, hash);

        name = nameRest(name);

        return name;
    }

    public SQLExpr expr() {
        if (lexer.token() == Token.STAR) {
            lexer.nextToken();

            SQLExpr expr = new SQLAllColumnExpr();

            if (lexer.token() == Token.DOT) {
                lexer.nextToken();
                accept(Token.STAR);
                return new SQLPropertyExpr(expr, "*");
            }

            return expr;
        }

        SQLExpr expr = primary();

        Token token = lexer.token();
        if (token == Token.COMMA) {
            return expr;
        } else if (token == Token.EQ) {
            expr = relationalRest(expr);
            expr = andRest(expr);
            expr = xorRest(expr);
            expr = orRest(expr);
            return expr;
        } else {
            return exprRest(expr);
        }
    }

    public SQLExpr primary() {

        if (lexer.token() == Token.LBRACKET) {
            lexer.nextToken();
            SQLExpr name = this.name();
            accept(Token.RBRACKET);
            return primaryRest(name);
        }

        return super.primary();
    }

    public SQLServerSelectParser createSelectParser() {
        return new SQLServerSelectParser(this);
    }

    public SQLExpr primaryRest(SQLExpr expr) {
        final Token token = lexer.token();
        if (token == Token.DOTDOT) {
            expr = nameRest((SQLName) expr);
        } else if (lexer.identifierEquals(FnvHash.Constants.VALUE)
                && expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
            if (identExpr.nameHashCode64() == FnvHash.Constants.NEXT) {
                lexer.nextToken();
                accept(Token.FOR);

                SQLName name = this.name();
                SQLSequenceExpr seq = new SQLSequenceExpr();
                seq.setSequence(name);
                seq.setFunction(SQLSequenceExpr.Function.NextVal);
                expr = seq;
            }
        }

        return super.primaryRest(expr);
    }

    protected SQLExpr dotRest(SQLExpr expr) {
        boolean backet = false;

        if (lexer.token() == Token.LBRACKET) {
            lexer.nextToken();
            backet = true;
        }

        expr = super.dotRest(expr);

        if (backet) {
            accept(Token.RBRACKET);
        }

        return expr;
    }

    public SQLName nameRest(SQLName expr) {
        if (lexer.token() == Token.DOTDOT) {
            lexer.nextToken();

            boolean backet = false;
            if (lexer.token() == Token.LBRACKET) {
                lexer.nextToken();
                backet = true;
            }
            String text = lexer.stringVal();
            lexer.nextToken();

            if (backet) {
                accept(Token.RBRACKET);
            }

            SQLServerObjectReferenceExpr owner = new SQLServerObjectReferenceExpr(expr);
            expr = new SQLPropertyExpr(owner, text);
        }

        return super.nameRest(expr);
    }

    public SQLServerTop parseTop() {
        if (lexer.token() == Token.TOP) {
            SQLServerTop top = new SQLServerTop();
            lexer.nextToken();

            boolean paren = false;
            if (lexer.token() == Token.LPAREN) {
                paren = true;
                lexer.nextToken();
            }

            if (lexer.token() == Token.LITERAL_INT) {
                top.setExpr(lexer.integerValue().intValue());
                lexer.nextToken();
            } else {
                top.setExpr(primary());
            }

            if (paren) {
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.PERCENT) {
                lexer.nextToken();
                top.setPercent(true);
            }

            return top;
        }

        return null;
    }
    
    protected SQLServerOutput parserOutput() {
        if (lexer.identifierEquals("OUTPUT")) {
            lexer.nextToken();
            SQLServerOutput output = new SQLServerOutput();

            final List<SQLSelectItem> selectList = output.getSelectList();
            for (;;) {
                final SQLSelectItem selectItem = parseSelectItem();
                selectList.add(selectItem);

                if (lexer.token() != Token.COMMA) {
                    break;
                }

                lexer.nextToken();
            }

            if (lexer.token() == Token.INTO) {
                lexer.nextToken();
                output.setInto(new SQLExprTableSource(this.name()));
                if (lexer.token() == (Token.LPAREN)) {
                    lexer.nextToken();
                    this.exprList(output.getColumns(), output);
                    accept(Token.RPAREN);
                }
            }
            return output;
        }
        return null;
    }

    public SQLSelectItem parseSelectItem() {
        SQLExpr expr;
        if (lexer.token() == Token.IDENTIFIER) {
            expr = new SQLIdentifierExpr(lexer.stringVal());
            lexer.nextTokenComma();

            if (lexer.token() != Token.COMMA) {
                expr = this.primaryRest(expr);
                expr = this.exprRest(expr);
            }
        } else {
            expr = this.expr();
        }
        final String alias = as();
        return new SQLSelectItem(expr, alias);
    }

    public SQLColumnDefinition createColumnDefinition() {
        SQLColumnDefinition column = new SQLColumnDefinition();
        column.setDbType(dbType);
        return column;
    }

    public SQLColumnDefinition parseColumnRest(SQLColumnDefinition column) {
        if (lexer.token() == Token.IDENTITY) {
            lexer.nextToken();

            SQLColumnDefinition.Identity identity = new SQLColumnDefinition.Identity();
            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
    
                SQLIntegerExpr seed = (SQLIntegerExpr) this.primary();
                accept(Token.COMMA);
                SQLIntegerExpr increment = (SQLIntegerExpr) this.primary();
                accept(Token.RPAREN);
                
                identity.setSeed((Integer) seed.getNumber());
                identity.setIncrement((Integer) increment.getNumber());
            }

            if (lexer.token() == Token.NOT) {
                lexer.nextToken();

                if (lexer.token() == Token.NULL) {
                    lexer.nextToken();
                    column.setDefaultExpr(new SQLNullExpr());
                } else {
                    accept(Token.FOR);
                    acceptIdentifier("REPLICATION ");
                    identity.setNotForReplication(true);
                }
            }

            column.setIdentity(identity);
        }

        return super.parseColumnRest(column);
    }
}
