package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataIndex;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.jca.cci.CciOperationNotSupportedException;

public class TeradataExprParser extends SQLExprParser {
    public final static String[] AGGREGATE_FUNCTIONS = {"AVG", "COUNT", "MAX", "MIN", "STDDEV", "SUM", "ROW_NUMBER"};

    public TeradataExprParser(String sql) {
        this(new TeradataLexer(sql));
        this.lexer.nextToken();
        this.dbType = JdbcConstants.TERADATA;
    }

    public TeradataExprParser(Lexer lexer) {
        super(lexer);
        this.aggregateFunctions = AGGREGATE_FUNCTIONS;
        this.dbType = JdbcConstants.TERADATA;
    }

    @Override
    public SQLConstraint parseConstraint() {
        SQLName name = null;

        if (lexer.token() == Token.CONSTRAINT) {
            lexer.nextToken();
            name = this.name();
        }

        SQLConstraint constraint;
        if (lexer.token() == Token.PRIMARY) {
            constraint = parsePrimaryKey();
        } else if (lexer.token() == Token.UNIQUE) {
            constraint = parseUnique();
        } else if (lexer.token() == Token.KEY) {
            constraint = parseUnique();
        } else if (lexer.token() == Token.FOREIGN) {
            constraint = parseForeignKey();
        } else if (lexer.token() == Token.CHECK) {
            constraint = parseCheck();
        } else if (lexer.token() == Token.INDEX) {
            constraint = parseIndex();
        } else {
            throw new ParserException("TODO : " + lexer.info());
        }

        constraint.setName(name);

        return constraint;
    }

    public SQLUnique parseIndex() {
        accept(Token.INDEX);

        SQLUnique unique = new SQLUnique();
        accept(Token.LPAREN);
        orderBy(unique.getColumns(), unique);
        accept(Token.RPAREN);

//        if (lexer.token() == Token.DISABLE) {
//            lexer.nextToken();
//            unique.setEnable(false);
//        } else if (lexer.token() == Token.ENABLE) {
//            lexer.nextToken();
//            unique.setEnable(true);
//        } else if (lexer.identifierEquals(FnvHash.Constants.VALIDATE)) {
//            lexer.nextToken();
//            unique.setValidate(Boolean.TRUE);
//        } else if (lexer.identifierEquals(FnvHash.Constants.NOVALIDATE)) {
//            lexer.nextToken();
//            unique.setValidate(Boolean.FALSE);
//        } else if (lexer.identifierEquals(FnvHash.Constants.RELY)) {
//            lexer.nextToken();
//            unique.setRely(Boolean.TRUE);
//        } else if (lexer.identifierEquals(FnvHash.Constants.NORELY)) {
//            lexer.nextToken();
//            unique.setRely(Boolean.FALSE);
//        }

        return unique;
    }
//    public SQLUnique parseIndex() {
//        accept(Token.INDEX);
//
//        TeradataIndex index = new TeradataIndex();
//        accept(Token.LPAREN);
//        exprList(index.getColumns(), index);
//        accept(Token.RPAREN);
//
//        return index;
//    }

}
