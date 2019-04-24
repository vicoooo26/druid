package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataIndex;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.jca.cci.CciOperationNotSupportedException;

import java.math.BigInteger;
import java.util.List;

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
        List<String> beforeComments = null;
        if (lexer.isKeepComments() && lexer.hasComment()) {
            beforeComments = lexer.readAndResetComments();
        }

        SQLExpr sqlExpr = null;

        final Token tok = lexer.token();

        switch (tok) {
            case LPAREN:
                lexer.nextToken();

                sqlExpr = expr();
                if (lexer.token() == Token.COMMA) {
                    SQLListExpr listExpr = new SQLListExpr();
                    listExpr.addItem(sqlExpr);
                    do {
                        lexer.nextToken();
                        listExpr.addItem(expr());
                    } while (lexer.token() == Token.COMMA);

                    sqlExpr = listExpr;
                }

                if (sqlExpr instanceof SQLBinaryOpExpr) {
                    ((SQLBinaryOpExpr) sqlExpr).setBracket(true);
                }

                accept(Token.RPAREN);

                if (lexer.token() == Token.UNION && sqlExpr instanceof SQLQueryExpr) {
                    SQLQueryExpr queryExpr = (SQLQueryExpr) sqlExpr;

                    SQLSelectQuery query = this.createSelectParser().queryRest(queryExpr.getSubQuery().getQuery());
                    queryExpr.getSubQuery().setQuery(query);
                }
                break;
            case INSERT:
                lexer.nextToken();
                if (lexer.token() != Token.LPAREN) {
                    throw new ParserException("syntax error. " + lexer.info());
                }
                sqlExpr = new SQLIdentifierExpr("INSERT");
                break;
            case IDENTIFIER:
                String ident = lexer.stringVal();
                long hash_lower = lexer.hash_lower();
                lexer.nextToken();

                if (hash_lower == FnvHash.Constants.DATE
                        && (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.VARIANT)
                        && (JdbcConstants.ORACLE.equals(dbType)
                        || JdbcConstants.POSTGRESQL.equals(dbType)
                        || JdbcConstants.MYSQL.equals(dbType))) {
                    SQLExpr literal = this.primary();
                    SQLDateExpr dateExpr = new SQLDateExpr();
                    dateExpr.setLiteral(literal);
                    sqlExpr = dateExpr;
                } else if (hash_lower == FnvHash.Constants.TIMESTAMP
                        && (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.VARIANT)
                        && !JdbcConstants.ORACLE.equals(dbType)) {
                    SQLTimestampExpr dateExpr = new SQLTimestampExpr(lexer.stringVal());
                    lexer.nextToken();
                    sqlExpr = dateExpr;
                } else if (JdbcConstants.MYSQL.equalsIgnoreCase(dbType) && ident.startsWith("0x") && (ident.length() % 2) == 0) {
                    sqlExpr = new SQLHexExpr(ident.substring(2));
                } else {
                    sqlExpr = new SQLIdentifierExpr(ident, hash_lower);
                }
                break;
            case NEW:
                throw new ParserException("TODO " + lexer.info());
            case LITERAL_INT:
                sqlExpr = new SQLIntegerExpr(lexer.integerValue());
                lexer.nextToken();
                break;
            case LITERAL_FLOAT:
                sqlExpr = lexer.numberExpr();
                lexer.nextToken();
                break;
            case LITERAL_CHARS: {
                sqlExpr = new SQLCharExpr(lexer.stringVal());

                if (JdbcConstants.MYSQL.equals(dbType)) {
                    lexer.nextTokenValue();

                    for (; ; ) {
                        if (lexer.token() == Token.LITERAL_ALIAS) {
                            String concat = ((SQLCharExpr) sqlExpr).getText();
                            concat += lexer.stringVal();
                            lexer.nextTokenValue();
                            sqlExpr = new SQLCharExpr(concat);
                        } else if (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_NCHARS) {
                            String concat = ((SQLCharExpr) sqlExpr).getText();
                            concat += lexer.stringVal();
                            lexer.nextTokenValue();
                            sqlExpr = new SQLCharExpr(concat);
                        } else {
                            break;
                        }
                    }
                } else {
                    lexer.nextToken();
                }
                break;
            }
            case LITERAL_NCHARS:
                sqlExpr = new SQLNCharExpr(lexer.stringVal());
                lexer.nextToken();

                if (JdbcConstants.MYSQL.equals(dbType)) {
                    SQLMethodInvokeExpr concat = null;
                    for (; ; ) {
                        if (lexer.token() == Token.LITERAL_ALIAS) {
                            if (concat == null) {
                                concat = new SQLMethodInvokeExpr("CONCAT");
                                concat.addParameter(sqlExpr);
                                sqlExpr = concat;
                            }
                            String alias = lexer.stringVal();
                            lexer.nextToken();
                            SQLCharExpr concat_right = new SQLCharExpr(alias.substring(1, alias.length() - 1));
                            concat.addParameter(concat_right);
                        } else if (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_NCHARS) {
                            if (concat == null) {
                                concat = new SQLMethodInvokeExpr("CONCAT");
                                concat.addParameter(sqlExpr);
                                sqlExpr = concat;
                            }

                            String chars = lexer.stringVal();
                            lexer.nextToken();
                            SQLCharExpr concat_right = new SQLCharExpr(chars);
                            concat.addParameter(concat_right);
                        } else {
                            break;
                        }
                    }
                }
                break;
            case VARIANT: {
                String varName = lexer.stringVal();
                lexer.nextToken();

                if (varName.equals(":") && lexer.token() == Token.IDENTIFIER && JdbcConstants.ORACLE.equals(dbType)) {
                    String part2 = lexer.stringVal();
                    lexer.nextToken();
                    varName += part2;
                }

                SQLVariantRefExpr varRefExpr = new SQLVariantRefExpr(varName);
                if (varName.startsWith(":")) {
                    varRefExpr.setIndex(lexer.nextVarIndex());
                }
                if (varRefExpr.getName().equals("@") && lexer.token() == Token.LITERAL_CHARS) {
                    varRefExpr.setName("@'" + lexer.stringVal() + "'");
                    lexer.nextToken();
                } else if (varRefExpr.getName().equals("@@") && lexer.token() == Token.LITERAL_CHARS) {
                    varRefExpr.setName("@@'" + lexer.stringVal() + "'");
                    lexer.nextToken();
                }
                sqlExpr = varRefExpr;
            }
            break;
            case DEFAULT:
                sqlExpr = new SQLDefaultExpr();
                lexer.nextToken();
                break;
            case DUAL:
            case KEY:
            case LIMIT:
            case SCHEMA:
            case COLUMN:
            case IF:
            case END:
            case COMMENT:
            case COMPUTE:
            case ENABLE:
            case DISABLE:
            case INITIALLY:
            case SEQUENCE:
            case USER:
            case EXPLAIN:
            case WITH:
            case GRANT:
            case REPLACE:
            case INDEX:
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
            case FULL:
            case TO:
            case IDENTIFIED:
            case PASSWORD:
            case BINARY:
            case WINDOW:
            case OFFSET:
            case SHARE:
            case START:
            case CONNECT:
            case MATCHED:
            case ERRORS:
            case REJECT:
            case UNLIMITED:
            case BEGIN:
            case EXCLUSIVE:
            case MODE:
            case ADVISE:
            case VIEW:
            case ESCAPE:
            case OVER:
            case ORDER:
            case CONSTRAINT:
            case TYPE:
            case OPEN:
            case REPEAT:
            case TABLE:
            case TRUNCATE:
            case EXCEPTION:
            case FUNCTION:
            case IDENTITY:
            case EXTRACT:
            case DESC:
            case DO:
            case GROUP:
            case MOD:
            case CONCAT:
                sqlExpr = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
                break;
            case CASE:
                SQLCaseExpr caseExpr = new SQLCaseExpr();
                lexer.nextToken();
                if (lexer.token() != Token.WHEN) {
                    caseExpr.setValueExpr(expr());
                }

                accept(Token.WHEN);
                SQLExpr testExpr = expr();
                accept(Token.THEN);
                SQLExpr valueExpr = expr();
                SQLCaseExpr.Item caseItem = new SQLCaseExpr.Item(testExpr, valueExpr);
                caseExpr.addItem(caseItem);

                while (lexer.token() == Token.WHEN) {
                    lexer.nextToken();
                    testExpr = expr();
                    accept(Token.THEN);
                    valueExpr = expr();
                    caseItem = new SQLCaseExpr.Item(testExpr, valueExpr);
                    caseExpr.addItem(caseItem);
                }

                if (lexer.token() == Token.ELSE) {
                    lexer.nextToken();
                    caseExpr.setElseExpr(expr());
                }

                accept(Token.END);

                sqlExpr = caseExpr;
                break;
            case EXISTS:
                lexer.nextToken();
                accept(Token.LPAREN);
                sqlExpr = new SQLExistsExpr(createSelectParser().select());
                accept(Token.RPAREN);
                break;
            case NOT:
                lexer.nextToken();
                if (lexer.token() == Token.EXISTS) {
                    lexer.nextToken();
                    accept(Token.LPAREN);
                    sqlExpr = new SQLExistsExpr(createSelectParser().select(), true);
                    accept(Token.RPAREN);
                } else if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();

                    SQLExpr notTarget = expr();

                    accept(Token.RPAREN);
                    notTarget = relationalRest(notTarget);
                    sqlExpr = new SQLNotExpr(notTarget);

                    return primaryRest(sqlExpr);
                } else {
                    SQLExpr restExpr = relational();
                    sqlExpr = new SQLNotExpr(restExpr);
                }
                break;
            case SELECT:
                SQLQueryExpr queryExpr = new SQLQueryExpr(
                        createSelectParser()
                                .select());
                sqlExpr = queryExpr;
                break;
            case CAST:
                lexer.nextToken();
                accept(Token.LPAREN);
                SQLCastExpr cast = new SQLCastExpr();
                cast.setExpr(expr());
                accept(Token.AS);
                cast.setDataType(parseDataType(false));
                accept(Token.RPAREN);
                sqlExpr = cast;

                break;
            case SUB:
                lexer.nextToken();
                switch (lexer.token()) {
                    case LITERAL_INT:
                        Number integerValue = lexer.integerValue();
                        if (integerValue instanceof Integer) {
                            int intVal = ((Integer) integerValue).intValue();
                            if (intVal == Integer.MIN_VALUE) {
                                integerValue = Long.valueOf(((long) intVal) * -1);
                            } else {
                                integerValue = Integer.valueOf(intVal * -1);
                            }
                        } else if (integerValue instanceof Long) {
                            long longVal = ((Long) integerValue).longValue();
                            if (longVal == 2147483648L) {
                                integerValue = Integer.valueOf((int) (((long) longVal) * -1));
                            } else {
                                integerValue = Long.valueOf(longVal * -1);
                            }
                        } else {
                            integerValue = ((BigInteger) integerValue).negate();
                        }
                        sqlExpr = new SQLIntegerExpr(integerValue);
                        lexer.nextToken();
                        break;
                    case LITERAL_FLOAT:
                        sqlExpr = lexer.numberExpr(true);
                        lexer.nextToken();
                        break;
                    case IDENTIFIER: // 当负号后面为字段的情况
                    case LITERAL_CHARS:
                    case LITERAL_ALIAS:
                        sqlExpr = new SQLIdentifierExpr(lexer.stringVal());
                        lexer.nextToken();

                        if (lexer.token() == Token.LPAREN || lexer.token() == Token.LBRACKET) {
                            sqlExpr = primaryRest(sqlExpr);
                        }
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Negative, sqlExpr);

                        break;
                    case QUES: {
                        SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr("?");
                        variantRefExpr.setIndex(lexer.nextVarIndex());
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Negative, variantRefExpr);
                        lexer.nextToken();
                        break;
                    }
                    case LPAREN:
                        lexer.nextToken();
                        sqlExpr = expr();
                        accept(Token.RPAREN);
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Negative, sqlExpr);
                        break;
                    case BANG:
                    case CAST:
                        sqlExpr = expr();
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Negative, sqlExpr);
                        break;
                    default:
                        throw new ParserException("TODO : " + lexer.info());
                }
                break;
            case PLUS:
                lexer.nextToken();
                switch (lexer.token()) {
                    case LITERAL_INT:
                        sqlExpr = new SQLIntegerExpr(lexer.integerValue());
                        lexer.nextToken();
                        break;
                    case LITERAL_FLOAT:
                        sqlExpr = lexer.numberExpr();
                        lexer.nextToken();
                        break;
                    case IDENTIFIER: // 当+号后面为字段的情况
                    case LITERAL_CHARS:
                    case LITERAL_ALIAS:
                        sqlExpr = new SQLIdentifierExpr(lexer.stringVal());
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Plus, sqlExpr);
                        lexer.nextToken();
                        break;
                    case LPAREN:
                        lexer.nextToken();
                        sqlExpr = expr();
                        accept(Token.RPAREN);
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Plus, sqlExpr);
                        break;
                    case SUB:
                    case CAST:
                        sqlExpr = expr();
                        sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Plus, sqlExpr);
                        break;
                    default:
                        throw new ParserException("TODO " + lexer.info());
                }
                break;
            case TILDE:
                lexer.nextToken();
                SQLExpr unaryValueExpr = primary();
                SQLUnaryExpr unary = new SQLUnaryExpr(SQLUnaryOperator.Compl, unaryValueExpr);
                sqlExpr = unary;
                break;
            case QUES:
                if (JdbcConstants.MYSQL.equals(dbType)) {
                    lexer.nextTokenValue();
                } else {
                    lexer.nextToken();
                }
                SQLVariantRefExpr quesVarRefExpr = new SQLVariantRefExpr("?");
                quesVarRefExpr.setIndex(lexer.nextVarIndex());
                sqlExpr = quesVarRefExpr;
                break;
            case LEFT:
                sqlExpr = new SQLIdentifierExpr("LEFT");
                lexer.nextToken();
                break;
            case RIGHT:
                sqlExpr = new SQLIdentifierExpr("RIGHT");
                lexer.nextToken();
                break;
            case DATABASE:
                sqlExpr = new SQLIdentifierExpr("DATABASE");
                lexer.nextToken();
                break;
            case LOCK:
                sqlExpr = new SQLIdentifierExpr("LOCK");
                lexer.nextToken();
                break;
            case NULL:
                sqlExpr = new SQLNullExpr();
                lexer.nextToken();
                break;
            case BANG:
                lexer.nextToken();
                SQLExpr bangExpr = primary();
                sqlExpr = new SQLUnaryExpr(SQLUnaryOperator.Not, bangExpr);
                break;
            case LITERAL_HEX:
                String hex = lexer.hexString();
                sqlExpr = new SQLHexExpr(hex);
                lexer.nextToken();
                break;
            case INTERVAL:
                sqlExpr = parseInterval();
                break;
            case COLON:
                lexer.nextToken();
                if (lexer.token() == Token.LITERAL_ALIAS) {
                    sqlExpr = new SQLVariantRefExpr(":\"" + lexer.stringVal() + "\"");
                    lexer.nextToken();
                }
                break;
            case ANY:
                sqlExpr = parseAny();
                break;
            case SOME:
                sqlExpr = parseSome();
                break;
            case ALL:
                sqlExpr = parseAll();
                break;
            case LITERAL_ALIAS:
                sqlExpr = parseAliasExpr(lexer.stringVal());
                lexer.nextToken();
                break;
            case EOF:
                throw new EOFParserException();
            case TRUE:
                lexer.nextToken();
                sqlExpr = new SQLBooleanExpr(true);
                break;
            case FALSE:
                lexer.nextToken();
                sqlExpr = new SQLBooleanExpr(false);
                break;
            case BITS: {
                String strVal = lexer.stringVal();
                lexer.nextToken();
                sqlExpr = new SQLBinaryExpr(strVal);
                break;
            }
            case CONTAINS:
                sqlExpr = inRest(null);
                break;
            case SET: {
                Lexer.SavePoint savePoint = lexer.mark();
                lexer.nextToken();
                if (lexer.token() == Token.LPAREN) {
                    sqlExpr = new SQLIdentifierExpr("SET");
                } else {
                    lexer.reset(savePoint);
                    throw new ParserException("ERROR. " + lexer.info());
                }
                break;
            }

            default:
                throw new ParserException("ERROR. " + lexer.info());
        }

        SQLExpr expr = primaryRest(sqlExpr);

        if (beforeComments != null) {
            expr.addBeforeComment(beforeComments);
        }

        return expr;
    }


}
