package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataUpdateStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;

public class TeradataUpdateParser extends SQLStatementParser {
    public TeradataUpdateParser(String sql) {
        super(new TeradataExprParser(sql));
    }

    public TeradataUpdateParser(Lexer lexer) {
        super(new TeradataExprParser(lexer));
    }

    public TeradataUpdateStatement parseUpdateStatement() {
        TeradataUpdateStatement update = new TeradataUpdateStatement();

        if (lexer.token() == Token.UPDATE) {
            lexer.nextToken();

            SQLTableSource tableSource = this.exprParser.createSelectParser().parseTableSource();
            update.setTableSource(tableSource);

//            if ((update.getAlias() == null) || (update.getAlias().length() == 0)) {
//                update.setAlias(as());
//            }
        }

        parseFrom(update);

        parseUpdateSet(update);

        parseWhere(update);

        System.out.println(update);

        return update;
    }

    private void parseFrom(TeradataUpdateStatement update) {
        if (lexer.token() == Token.FROM) {
            lexer.nextToken();

//			SQLSelectParser parser = new SQLSelectParser(lexer);
            update.setFrom(parseTableSource());
        }
    }

    private SQLTableSource parseTableSource() {
        SQLExprTableSource tableReference = new SQLExprTableSource();
        tableReference.setExpr(this.exprParser.expr());
        // for join query
//		SQLTableSource tableSrc = tableReference;
        SQLTableSource tableSrc = parseTableSourceRest(tableReference);
        if (lexer.hasComment() && lexer.isKeepComments()) {
            tableSrc.addAfterComment(lexer.readAndResetComments());
        }
        return tableSrc;
    }

    private SQLTableSource parseTableSourceRest(SQLTableSource tableSource) {
        if ((tableSource.getAlias() == null) || (tableSource.getAlias().length() == 0)) {
            if (lexer.token() != Token.LEFT && lexer.token() != Token.RIGHT && lexer.token() != Token.FULL
                    && !identifierEquals("STRAIGHT_JOIN") && !identifierEquals("CROSS")) {
                String alias = as();
                if (alias != null) {
                    tableSource.setAlias(alias);
                    return parseTableSourceRest(tableSource);
                }
            }
        }

        SQLJoinTableSource.JoinType joinType = null;

        if (lexer.token() == Token.LEFT) {
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }

            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN;
        } else if (lexer.token() == Token.RIGHT) {
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN;
        } else if (lexer.token() == Token.FULL) {
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.FULL_OUTER_JOIN;
        } else if (lexer.token() == Token.INNER) {
            lexer.nextToken();
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.INNER_JOIN;
        } else if (lexer.token() == Token.JOIN) {
            lexer.nextToken();
            joinType = SQLJoinTableSource.JoinType.JOIN;
        } else if (lexer.token() == Token.COMMA) {
            lexer.nextToken();
            joinType = SQLJoinTableSource.JoinType.COMMA;
        } else if (identifierEquals("STRAIGHT_JOIN")) {
            lexer.nextToken();
            joinType = SQLJoinTableSource.JoinType.STRAIGHT_JOIN;
        } else if (identifierEquals("CROSS")) {
            lexer.nextToken();
            if (lexer.token() == Token.JOIN) {
                lexer.nextToken();
                joinType = SQLJoinTableSource.JoinType.CROSS_JOIN;
            } else if (identifierEquals("APPLY")) {
                lexer.nextToken();
                joinType = SQLJoinTableSource.JoinType.CROSS_APPLY;
            }
        } else if (lexer.token() == Token.OUTER) {
            lexer.nextToken();
            if (identifierEquals("APPLY")) {
                lexer.nextToken();
                joinType = SQLJoinTableSource.JoinType.OUTER_APPLY;
            }
        }

        if (joinType != null) {
            SQLJoinTableSource join = new SQLJoinTableSource();
            join.setLeft(tableSource);
            join.setJoinType(joinType);
            join.setRight(parseTableSource());

            if (lexer.token() == Token.ON) {
                lexer.nextToken();
                join.setCondition(this.exprParser.expr());
            } else if (identifierEquals("USING")) {
                lexer.nextToken();
                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();
                    this.exprParser.exprList(join.getUsing(), join);
                    accept(Token.RPAREN);
                } else {
                    join.getUsing().add(this.exprParser.expr());
                }
            }

            return parseTableSourceRest(join);
        }

        return tableSource;
    }

    private void parseWhere(TeradataUpdateStatement update) {
        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            update.setWhere(this.exprParser.expr());
        }
    }
}
