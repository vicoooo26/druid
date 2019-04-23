package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateTableStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLCreateTableParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

public class TeradataCreateTableParser extends SQLCreateTableParser {
    public TeradataCreateTableParser(String sql) {
        super(new TeradataExprParser(sql));
    }

    public TeradataCreateTableParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public SQLCreateTableStatement parseCreateTable() {
        return parseCreateTable(true);
    }

    public TeradataExprParser getExprParser() {
        return (TeradataExprParser) exprParser;
    }

    public TeradataCreateTableStatement parseCreateTable(boolean acceptCreate) {
        TeradataCreateTableStatement stmt = new TeradataCreateTableStatement();

        if (acceptCreate) {
            accept(Token.CREATE);
        }

        if (lexer.token() == Token.SET) {
            stmt.setType(TeradataCreateTableStatement.Type.SET);
            lexer.nextToken();
        }

        if (lexer.token() == Token.MULTISET) {
            stmt.setType(TeradataCreateTableStatement.Type.MULTISET);
            lexer.nextToken();
        }

        if (lexer.token() == Token.VOLATILE) {
            lexer.nextToken();
        }

        accept(Token.TABLE);

        stmt.setName(this.exprParser.name());

        if (lexer.token() == (Token.AS)) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.COMMA) {

        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            if (lexer.token() == Token.SELECT || lexer.token() == Token.SEL) {
                SQLSelect query = new TeradataSelectParser(this.exprParser).select();
                stmt.setSelect(query);
            } else {
                for (; ; ) {
                    SQLColumnDefinition column = this.exprParser.parseColumn();
                    stmt.getTableElementList().add(column);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();

                        if (lexer.token() == Token.RPAREN) {
                            break;
                        }
                        continue;
                    }
                    break;
                }
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.SELECT || lexer.token() == Token.SEL) {
            SQLSelect query = new TeradataSelectParser(this.exprParser).select();
            stmt.setSelect(query);
        }

        for (; ; ) {
            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                if (identifierEquals("DATA")) {
                    lexer.nextToken();
                    stmt.setWithData(true);
                } else if (identifierEquals("NO")) {
                    lexer.nextToken();
                    acceptIdentifier("DATA");
                    stmt.setWithNoData(true);
                }
                continue;
            } else if (lexer.token() == Token.ON) {
                lexer.nextToken();
                accept(Token.COMMIT);
                stmt.setOnCommit(true);
                continue;
            } else if (identifierEquals("PRESERVE")) {
                lexer.nextToken();
                acceptIdentifier("ROWS");
                stmt.setPreserveRows(true);
                continue;
            } else if (lexer.token() == Token.UNIQUE) {
                // deal with "unique primary index(...)" or
                // "unique index(...)"
                stmt.setUniqueIndex(true);
                lexer.nextToken();

                if (lexer.token() == Token.PRIMARY) {
                    continue;
                } else if (lexer.token() == Token.INDEX) {
                    SQLConstraint constraint = this.exprParser.parseConstraint();
                    constraint.setParent(stmt);
                    stmt.setConstraints(constraint);
                    continue;
                } else {
                    throw new ParserException("Unknown token: " + lexer.stringVal());
                }
            } else if (lexer.token() == Token.PRIMARY) {
                // deal with "primary index(...)"
                lexer.nextToken();
                if (lexer.token() == Token.INDEX) {
                    SQLConstraint constraint = this.exprParser.parseConstraint();
                    constraint.setParent(stmt);
                    stmt.setConstraints(constraint);
                }
                continue;
            } else if (identifierEquals("NO")) {
                // deal with "no primary index"
                // or "no index"
                lexer.nextToken();
                continue;
            }
            break;
        }

        return stmt;
    }
}
