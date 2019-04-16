package sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import sql.druid.DruidSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SqlParserFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlParserFactory.class);

    private static class Holder {

        private static final SqlParserFactory INSTANCE = new SqlParserFactory();
    }

    public static final SqlParserFactory getInstance() {
        return SqlParserFactory.Holder.INSTANCE;
    }

    private SqlParserFactory() {
    }

    public ISqlParser createSqlParser(SqlParseType sqlParseType) {
        switch (sqlParseType) {
            case Hive:
            case Inceptor:
                return new DruidSqlParser(sqlParseType.getType());
            case HBase:
            case Hyperbase:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
            case Oracle:
            case DB2:
            case Teradata:
            case MySQL:
            case SqlServer:
                return new DruidSqlParser(sqlParseType.getType());
            default:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
        }
    }

    public static void main(String[] args) {
        db2Lineage();
//        mysqlLineage();
//        oracleLineage();
    }

    protected static void mysqlLineage() {
        // 创建数据库
        String mysql1 = "create database test1";

        // 创建table，添加样例数据
        String mysql2 = "create table test1.table1(id bigint(20) unsigned primary key, name varchar(10) not null default '', age bigint(10) default 0, address varchar(200) default '')";

        String mysql3 = "insert into test1.table1 values (1, 'jack1', 18, 'shanghai')";
        String mysql4 = "insert into test1.table1 values (2, 'jack2', 19, 'shanghai')";
        String mysql5 = "create table test1.table2 as select id,name,age,address from test1.table1";
        String mysql6 = "create table test1.table3 as select id,name,age,address from test1.table2";

        // 创建view
        String mysql7 = "create view test1.test_view as select * from test1.table1";

        // 创建procedure
        String mysql8 = "create or replace procedure test1.foobar(inout msg varchar(100))" +
                " begin" +
                " set msg = concat(@msg, \"never gonna let you down\");" +
                " end";

        // 创建空参数procedure
        String mysql9 = "create procedure test1.empty_param()" +
                " begin" +
                " end";
        List<SQLStatement> mysql1Result = SQLUtils.parseStatements(mysql1, "mysql");
        List<SQLStatement> mysql2Result = SQLUtils.parseStatements(mysql2, "mysql");
        List<SQLStatement> mysql3Result = SQLUtils.parseStatements(mysql3, "mysql");
        List<SQLStatement> mysql4Result = SQLUtils.parseStatements(mysql4, "mysql");
        List<SQLStatement> mysql5Result = SQLUtils.parseStatements(mysql5, "mysql");
        List<SQLStatement> mysql6Result = SQLUtils.parseStatements(mysql6, "mysql");
        List<SQLStatement> mysql7Result = SQLUtils.parseStatements(mysql7, "mysql");
        List<SQLStatement> mysql8Result = SQLUtils.parseStatements(mysql8, "mysql");
        List<SQLStatement> mysql9Result = SQLUtils.parseStatements(mysql9, "mysql");
        System.out.println("");
    }

    protected static void oracleLineage() {
        ISqlParser oracleSqlParser = SqlParserFactory.getInstance().createSqlParser(SqlParseType.Oracle);
        String oracleSql1 = "create table STUDENT\n" +
                "(\n" +
                " ID NUMBER(10) not null\n" +
                " primary key,\n" +
                " NAME VARCHAR2(10) not null,\n" +
                " SEX VARCHAR2(4) default '男'\n" +
                " check (sex in('男','女'))\n" +
                ");";

        List<SQLStatement> oracleSql1Result = SQLUtils.parseStatements(oracleSql1, "oracle");


        String oracleSql2 = "create table test1.table1(id number, name varchar2(10), age number, address varchar2(10));";
        List<SQLStatement> oracleSql2Result = SQLUtils.parseStatements(oracleSql2, "oracle");

        String oracleSql3 = "create table test1.table2 as select id,name,age,address from test1.table1;";
        List<SQLStatement> oracleSql3Result = SQLUtils.parseStatements(oracleSql3, "oracle");

        String oracleSql4 = "create table test1.table3 as select id,name,age,address from test1.table2;";
        List<SQLStatement> oracleSql4Result = SQLUtils.parseStatements(oracleSql4, "oracle");


        String oracleSql5 = "CREATE VIEW TEST1.TEST_VIEW AS SELECT * FROM TEST1.STUDENT;";
        List<SQLStatement> oracleSql5Result = SQLUtils.parseStatements(oracleSql5, "oracle");

        String oracleSql6 = "create or replace procedure test_procedure(a Date, b VARCHAR2, c INT) is\n" +
                " x int;\n" +
                "begin\n" +
                " x := 123;\n" +
                "end;";
        List<SQLStatement> oracleSql6Result = SQLUtils.parseStatements(oracleSql6, "oracle");

        String oracleSql7 = "create or replace\n" +
                "PACKAGE Test_package is\n" +
                "function test_function(a DATE, b VARCHAR2, c VARCHAR2, d VARCHAR2,\n" +
                "e VARCHAR2, f VARCHAR2, g VARCHAR2, h VARCHAR2, i VARCHAR2,\n" +
                "j VARCHAR2, k VARCHAR2, l VARCHAR2) return BOOLEAN;\n" +
                "procedure test_procedure;\n" +
                "procedure test_procedure(a Date, b VARCHAR2);\n" +
                "procedure test_procedure(a Date, b VARCHAR2, c INT);\n" +
                "end Test_package;\n" +
                "\n" +
                "create or replace\n" +
                "PACKAGE body Test_package is\n" +
                "\n" +
                "function test_function(a DATE, b VARCHAR2, c VARCHAR2, d VARCHAR2,\n" +
                "e VARCHAR2, f VARCHAR2, g VARCHAR2, h VARCHAR2, i VARCHAR2,\n" +
                "j VARCHAR2, k VARCHAR2, l VARCHAR2) return BOOLEAN is\n" +
                "begin\n" +
                " null;\n" +
                " return true;\n" +
                "end;\n" +
                "\n" +
                "procedure test_procedure is\n" +
                " x int;\n" +
                "begin\n" +
                " x := 123;\n" +
                "end;\n" +
                "\n" +
                "procedure test_procedure(a Date, b VARCHAR2) is\n" +
                " x int;\n" +
                "begin\n" +
                " x := 123;\n" +
                "end;\n" +
                "\n" +
                "procedure test_procedure(a Date, b VARCHAR2, c INT) is\n" +
                " x int;\n" +
                "begin\n" +
                " x := 123;\n" +
                "end;\n" +
                "\n" +
                "end Test_package;";
        List<SQLStatement> oracleSql7Result = SQLUtils.parseStatements(oracleSql7, "oracle");
        System.out.println("");

//        Map<String, List<String>> sourceTargetTableMapOracle1 = oracleSqlParser.getTargetSourceTableMap(oracleSql1);
//        Map<String, List<String>> sourceTargetTableMapOracle2 = oracleSqlParser.getTargetSourceTableMap(oracleSql2);
    }

    protected static void db2Lineage() {
        ISqlParser db2SqlParser = SqlParserFactory.getInstance().createSqlParser(SqlParseType.DB2);
        // parser lexer done,  lineage done
        String db2Sql1 = "create table table1(id bigint, name varchar(20), age int, address varchar(50))";
        String db2FormatSql1 = SQLUtils.format(db2Sql1, "db2");
        List<SQLStatement> db2Statements1 = SQLUtils.parseStatements(db2FormatSql1, "db2");
        System.out.println(db2FormatSql1);
        System.out.println("\n");

        // parser lexer done, lineage not done
        String db2Sql2 = "CREATE TABLE test1.table2 LIKE test1.table1";
        String db2FormatSql2 = SQLUtils.format(db2Sql2, "db2");
        List<SQLStatement> db2Statements2 = SQLUtils.parseStatements(db2FormatSql2, "db2");
        System.out.println(db2FormatSql2);
        System.out.println("\n");

        // compare mysql to db2
//        String mysqlFormatSql2 = SQLUtils.format(db2Sql2,"mysql");
//        List<SQLStatement> mysqlStatements = SQLUtils.parseStatements(db2Sql2, "mysql");
//        System.out.println(mysqlFormatSql2);

        // parser lexer done,  lineage not done - view as source not found
        String db2Sql3 = "CREATE VIEW test1.view1 AS (SELECT id,name,age,address FROM test1.table1)";
        String db2FormatSql3 = SQLUtils.format(db2Sql3, "db2");
        List<SQLStatement> db2Statements3 = SQLUtils.parseStatements(db2FormatSql3, "db2");
        System.out.println(db2FormatSql3);
        System.out.println("\n");

        // parser lexer done,  lineage done
        String db2Sql4 = "create table table2 as (select id,name,age,address from table1) definition only";
        String db2FormatSql4 = SQLUtils.format(db2Sql4, "db2");
        List<SQLStatement> db2Statements4 = SQLUtils.parseStatements(db2FormatSql4, "db2");
        System.out.println(db2FormatSql4);
        System.out.println("\n");

        // parser lexer done,  lineage done
        String db2Sql5 = "create table table3 as (select * from table2) definition only";
        String db2FormatSql5 = SQLUtils.format(db2Sql5, "db2");
        List<SQLStatement> db2Statements5 = SQLUtils.parseStatements(db2FormatSql5, "db2");
        System.out.println(db2FormatSql5);
        System.out.println("\n");

        // parser lexer done,  lineage not done
        String db2Sql6 = "CREATE OR REPLACE PROCEDURE INSERT_TABLE1 (IN in_id INTEGER,IN in_name VARCHAR(20)) BEGIN END";
        String db2FormatSql6 = SQLUtils.format(db2Sql6, "db2");
        List<SQLStatement> db2Statements6 = SQLUtils.parseStatements(db2FormatSql6, "db2");
        System.out.println(db2FormatSql6);
        System.out.println("\n");

        // parser lexer done,  lineage not done
        String db2Sql7 = "insert into table1(id,name,age,address) values(1, 'mark', 18, 'shanghai')";
        String db2FormatSql7 = SQLUtils.format(db2Sql7, "db2");
        List<SQLStatement> db2Statements7 = SQLUtils.parseStatements(db2FormatSql7, "db2");
        System.out.println(db2FormatSql7);
        System.out.println("\n");

        Map<String, List<String>> sourceTargetTableMapDB2_1 = db2SqlParser.getTargetSourceTableMap(db2Sql1);

        Map<String, List<String>> sourceTargetTableMapDB2_2 = db2SqlParser.getTargetSourceTableMap(db2Sql2);

        Map<String, List<String>> sourceTargetTableMapDB2_3 = db2SqlParser.getTargetSourceTableMap(db2Sql3);

        Map<String, List<String>> sourceTargetTableMapDB2_4 = db2SqlParser.getTargetSourceTableMap(db2Sql4);

        Map<String, List<String>> sourceTargetTableMapDB2_5 = db2SqlParser.getTargetSourceTableMap(db2Sql5);

        System.out.println("");
    }
}
