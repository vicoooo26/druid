package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelect;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;

public class TeradataSelectParser extends SQLSelectParser {
    public TeradataSelectParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public TeradataSelectParser(String sql) {
        this(new TeradataExprParser(sql));
    }

    public TeradataSelect select() {
        TeradataSelect select = new TeradataSelect();

        if (lexer.token() == Token.WITH) {
            SQLWithSubqueryClause with = this.parseWith();
            select.setWithSubQuery(with);
        }

        SQLSelectQuery query = query();
        select.setQuery(query);

        SQLOrderBy orderBy = this.parseOrderBy();

        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;

            if (queryBlock.getOrderBy() == null) {
                queryBlock.setOrderBy(orderBy);
            } else {
                select.setOrderBy(orderBy);
            }

            if (orderBy != null) {
                parseFetchClause(queryBlock);
            }
        } else {
            select.setOrderBy(orderBy);
        }

        while (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(select.getHints());
        }

        if (lexer.token() == Token.SAMPLE) {
            lexer.nextToken();
            select.setSample(this.exprParser.expr());
        }

        if (lexer.token() == Token.TOP) {
            lexer.nextToken();
            select.setTop(this.exprParser.expr());
        }
        return select;
    }

    public SQLSelectQuery query() {
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            SQLSelectQuery select = query();
            accept(Token.RPAREN);

            return queryRest(select);
        }

        TeradataSelectQueryBlock queryBlock = new TeradataSelectQueryBlock();

        if (lexer.token() == Token.SEL
                || lexer.token() == Token.SELECT) {

            lexer.nextToken();

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
            }

            if (lexer.token() == (Token.DISTINCT)) {
                queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
                lexer.nextToken();
            } else if (lexer.token() == (Token.ALL)) {
                queryBlock.setDistionOption(SQLSetQuantifier.ALL);
                lexer.nextToken();
            } else if (lexer.token() == Token.UNIQUE) {
                queryBlock.setDistionOption(SQLSetQuantifier.UNIQUE);
                lexer.nextToken();
            }
            parseSelectList(queryBlock);
        }
        parseFrom(queryBlock);

        parseWhere(queryBlock);

        parseGroupBy(queryBlock);

        parseWhere(queryBlock);

        parserQualify(queryBlock);

        parseWhere(queryBlock);

        queryBlock.setOrderBy(this.exprParser.parseOrderBy());

        return queryRest(queryBlock);
    }

    private void parserQualify(TeradataSelectQueryBlock queryBlock) {
        if (lexer.token() != Token.QUALIFY) {
            return;
        }

        lexer.nextToken();

        if (lexer.token() == Token.LPAREN) {
            accept(Token.LPAREN);
        }
        if (lexer.token() == Token.LITERAL_INT) {
            lexer.nextToken();
            // possibly =, >=, <=
            // ignore this for now
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                accept(Token.LPAREN);
            }
            if (lexer.token() == Token.IDENTIFIER) {
                SQLExpr expr = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
                if (lexer.token() != Token.COMMA) {
                    expr = this.exprParser.primaryRest(expr);
                }
                if (lexer.token() == Token.RPAREN) {
                    accept(Token.RPAREN);
                }
                // TODO: add qualify clause into queryBlock
                queryBlock.setQualifyClause(expr);
            } else {
                throw new ParserException("not support token:" + lexer.token());
            }
        } else if (lexer.token() == Token.IDENTIFIER) {
            SQLExpr expr = new SQLIdentifierExpr(lexer.stringVal());
            lexer.nextToken();
            if (lexer.token() != Token.COMMA) {
                expr = this.exprParser.primaryRest(expr);
            }
            if (lexer.token() == Token.RPAREN) {
                accept(Token.RPAREN);
            }
            lexer.nextToken();
            lexer.nextToken();
            // TODO: add qualify clause into queryBlock
            queryBlock.setQualifyClause(expr);
        } else {
            throw new ParserException("not support token:" + lexer.token());
        }


    }

}
