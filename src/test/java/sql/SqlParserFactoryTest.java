package sql;

import com.alibaba.druid.sql.SQLUtils;

import static org.junit.Assert.*;

import org.junit.Test;


public class SqlParserFactoryTest {


    @Test
    public void db2Lineage() {
        ISqlParser sqlParser = SqlParserFactory.getInstance().createSqlParser(SqlParseType.DB2);

        String db2Sql1 = "create table table1(id bigint, name varchar(20), age int, address varchar(50))";

        String db2Sql2 = "CREATE TABLE test1.table2 LIKE test1.table1";

        String db2Sql3 = "CREATE VIEW test1.view1 AS (SELECT id,name,age,address FROM test1.table1)";

        String db2Sql4 = "create table table2 as (select id,name,age,address from table1) definition only";

        String str1 = "CREATE TABLE DB2INST1.TABLE5 (\n"
                + "MAT_DT DATE NULL, MAT_DTS TIMESTAMP NULL, PBC_APRV_NO VARCHAR(32) NOT NULL, BAL DECIMAL(26, 2) NULL\n"
                + ")";
        String str2 = "CREATE TABLE DB2INST1.TABLE1 (\n"
                + "ID BIGINT(8) NULL, NAME VARCHAR(20) NULL, AGE INTEGER(4) NULL, ADDRESS VARCHAR(50) NULL\n"
                + ")";

        String str3 = "CREATE TABLE DB2INST1.TABLE2 (\n"
                + "ID BIGINT(8) NULL, NAME VARCHAR(20) NULL, AGE INTEGER(4) NULL, ADDRESS VARCHAR(50) NULL\n"
                + ")";

        String str4 = "CREATE TABLE DB2INST1.TABLE3 (\n"
                + "ID BIGINT(8) NULL, NAME VARCHAR(20) NULL, AGE INTEGER(4) NULL, ADDRESS VARCHAR(50) NULL\n"
                + ")";

        String str5 = "CREATE TABLE DB2INST1.TABLE4 (\n"
                + "ID BIGINT(8) NULL, NAME VARCHAR(20) NULL, AGE INTEGER(4) NULL, ADDRESS VARCHAR(50) NULL\n"
                + ")";

        String str6 = "create view view1 as (select * from table1)";

        String str7 = "CREATE\n"
                + "    VIEW view2 AS(\n"
                + "        SELECT\n"
                + "            *\n"
                + "        FROM\n"
                + "            table1\n"
                + "    )";

        String str8 = "CREATE\n"
                + "    VIEW view3 AS(\n"
                + "        SELECT\n"
                + "            *\n"
                + "        FROM\n"
                + "            table1\n"
                + "    )";
        assertNotNull(sqlParser.getTargetSourceTableMap(db2Sql1));
        assertNotNull(sqlParser.getTargetSourceTableMap(db2Sql2));
        assertNotNull(sqlParser.getTargetSourceTableMap(db2Sql3));
        assertNotNull(sqlParser.getTargetSourceTableMap(db2Sql4));

        assertNotNull(sqlParser.getTargetSourceTableMap(str1));
        assertNotNull(sqlParser.getTargetSourceTableMap(str2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str3));
        assertNotNull(sqlParser.getTargetSourceTableMap(str4));

        assertNotNull(sqlParser.getTargetSourceTableMap(str5));
        assertNotNull(sqlParser.getTargetSourceTableMap(str6));
        assertNotNull(sqlParser.getTargetSourceTableMap(str7));
        assertNotNull(sqlParser.getTargetSourceTableMap(str8));

    }

    @Test
    public void mysqlLineage() {
        ISqlParser sqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.MySQL);
        String str1 = "CREATE TABLE `database_table_column123` (\n"
                + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
                + "  `authority` varchar(100) NOT NULL DEFAULT '' COMMENT '权限',\n"
                + "  `owner` varchar(200) NOT NULL DEFAULT '0' COMMENT '创建者',\n"
                + "  `data_entity_type` varchar(20) NOT NULL DEFAULT '' COMMENT '数据实体类型',\n"
                + "  `database_table_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '数据表id',\n"
                + "  `name` varchar(200) NOT NULL DEFAULT '' COMMENT '列名称',\n"
                + "  `type` longtext COMMENT '列类型',\n"
                + "  `order` int(11) NOT NULL DEFAULT '0' COMMENT '列顺序',\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='数据列信息'";

        String str2 = "CREATE TABLE `database_table_column456` (\n"
                + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
                + "  `authority` varchar(100) NOT NULL DEFAULT '' COMMENT '权限',\n"
                + "  `owner` varchar(200) NOT NULL DEFAULT '0' COMMENT '创建者',\n"
                + "  `data_entity_type` varchar(20) NOT NULL DEFAULT '' COMMENT '数据实体类型',\n"
                + "  `database_table_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '数据表id',\n"
                + "  `name` varchar(200) NOT NULL DEFAULT '' COMMENT '列名称',\n"
                + "  `type` longtext COMMENT '列类型',\n"
                + "  `order` int(11) NOT NULL DEFAULT '0' COMMENT '列顺序',\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据列信息'";

        String str3 = "CREATE TABLE `database_table_column7123123890` (\n"
                + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
                + "  `authority` varchar(100) NOT NULL DEFAULT '' COMMENT '权限',\n"
                + "  `owner` varchar(200) NOT NULL DEFAULT '0' COMMENT '创建者',\n"
                + "  `data_entity_type` varchar(20) NOT NULL DEFAULT '' COMMENT '数据实体类型',\n"
                + "  `database_table_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '数据表id',\n"
                + "  `name` varchar(200) NOT NULL DEFAULT '' COMMENT '列名称',\n"
                + "  `type` longtext COMMENT '列类型',\n"
                + "  `order` int(11) NOT NULL DEFAULT '0' COMMENT '列顺序',\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据列信息'";

        String str4 = "create view test_again.database_table_column123_view as select `test_again`.`database_table_column123`.`id` AS `id`,`test_again`.`database_table_column123`.`authority` AS `authority`,`test_again`.`database_table_column123`.`owner` AS `owner`,`test_again`.`database_table_column123`.`data_entity_type` AS `data_entity_type`,`test_again`.`database_table_column123`.`database_table_id` AS `database_table_id`,`test_again`.`database_table_column123`.`name` AS `name`,`test_again`.`database_table_column123`.`type` AS `type`,`test_again`.`database_table_column123`.`order` AS `order` from `test_again`.`database_table_column123`";

        String str5 = "create view test_again.view1 as select `test_again`.`database_table_column123`.`id` AS `id`,`test_again`.`database_table_column123`.`owner` AS `owner` from `test_again`.`database_table_column123`";

        assertNotNull(sqlParser.getTargetSourceTableMap(str1));
        assertNotNull(sqlParser.getTargetSourceTableMap(str2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str3));
        assertNotNull(sqlParser.getTargetSourceTableMap(str4));
        assertNotNull(sqlParser.getTargetSourceTableMap(str5));

    }

    @Test
    public void oracleLineage() {
        ISqlParser sqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.Oracle);

        String oracleSql1 = "create table test1.table1(id number, name varchar2(10), age number, address varchar2(10));";

        String oracleSql2 = "create table test1.table2 as select id,name,age,address from test1.table1;";

        String str1 = "CREATE TABLE \"TEST1\".\"TABLE1\" \n"
                + "   (\t\"ID\" NUMBER, \n"
                + "\t\"NAME\" VARCHAR2(10), \n"
                + "\t\"AGE\" NUMBER, \n"
                + "\t\"ADDRESS\" VARCHAR2(10)\n"
                + "   ) SEGMENT CREATION IMMEDIATE \n"
                + "  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n"
                + "  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n"
                + "  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n"
                + "  TABLESPACE \"SYSTEM\" ";
        String str2 = "CREATE TABLE \"TEST1\".\"TABLE2\" \n"
                + "   (\t\"ID\" NUMBER, \n"
                + "\t\"NAME\" VARCHAR2(10), \n"
                + "\t\"AGE\" NUMBER, \n"
                + "\t\"ADDRESS\" VARCHAR2(10)\n"
                + "   ) SEGMENT CREATION IMMEDIATE \n"
                + "  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n"
                + "  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n"
                + "  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n"
                + "  TABLESPACE \"SYSTEM\" ";

        String str3 = "CREATE TABLE \"TEST1\".\"TABLE3\" \n"
                + "   (\t\"ID\" NUMBER, \n"
                + "\t\"NAME\" VARCHAR2(10), \n"
                + "\t\"AGE\" NUMBER, \n"
                + "\t\"ADDRESS\" VARCHAR2(10)\n"
                + "   ) SEGMENT CREATION IMMEDIATE \n"
                + "  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n"
                + "  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n"
                + "  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n"
                + "  TABLESPACE \"SYSTEM\" ";

        String str4 =
                "CREATE OR REPLACE FORCE VIEW \"TEST1\".\"TEST_VIEW\" (\"ID\", \"NAME\", \"SEX\") AS \n"
                        + "  SELECT \"ID\",\"NAME\",\"SEX\" FROM TEST1.STUDENT";

        assertNotNull(sqlParser.getTargetSourceTableMap(oracleSql1));
        assertNotNull(sqlParser.getTargetSourceTableMap(oracleSql2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str1));
        assertNotNull(sqlParser.getTargetSourceTableMap(str2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str3));
        assertNotNull(sqlParser.getTargetSourceTableMap(str4));

    }

    @Test
    public void sqlServerLineage() {
        ISqlParser sqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.SqlServer);

        String str1 = "CREATE TABLE test1.table1 (\n"
                + " id int  NULL,\n"
                + " name varchar(20)  NULL,\n"
                + " age int  NULL,\n"
                + " address varchar(50)  NULL)";
        String str2 = "CREATE TABLE test1.table2 (\n"
                + " id int  NULL,\n"
                + " name varchar(20)  NULL,\n"
                + " age int  NULL,\n"
                + " address varchar(50)  NULL)";
        String str3 = "CREATE TABLE test1.table3 (\n"
                + " id int  NULL,\n"
                + " name varchar(20)  NULL,\n"
                + " age int  NULL,\n"
                + " address varchar(50)  NULL)";
        String str4 = "create view test1.view1 as select * from test1.table1\n";
        String str5 = "create view test1.view2 as select * from test1.view1\n";
        String str6 = "create view test1.view3 as select * from test1.view2\n";

        assertNotNull(sqlParser.getTargetSourceTableMap(str1));
        assertNotNull(sqlParser.getTargetSourceTableMap(str2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str3));
        assertNotNull(sqlParser.getTargetSourceTableMap(str4));
        assertNotNull(sqlParser.getTargetSourceTableMap(str5));
        assertNotNull(sqlParser.getTargetSourceTableMap(str6));


    }

    @Test
    public void teradataLineage() {
        ISqlParser sqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.Teradata);
        String str0 = "CREATE MULTISET TABLE database_teradata1.table1 ,FALLBACK ,\n"
                + "     NO BEFORE JOURNAL,\n"
                + "     NO AFTER JOURNAL,\n"
                + "     CHECKSUM = DEFAULT,\n"
                + "     DEFAULT MERGEBLOCKRATIO,\n"
                + "     MAP = TD_MAP1\n"
                + "     (\n"
                + "      id INTEGER,\n"
                + "      name VARCHAR(30) CHARACTER SET LATIN CASESPECIFIC,\n"
                + "      age INTEGER,\n"
                + "      address VARCHAR(30) CHARACTER SET LATIN CASESPECIFIC,\n"
                + "      address1 VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,\n"
                + "      address2 VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC)\n"
                + "UNIQUE PRIMARY INDEX ( id );";

        String str1 = "CREATE MULTISET TABLE database_teradata1.table1 ,FALLBACK ,\n"
                + "     NO BEFORE JOURNAL,\n"
                + "     NO AFTER JOURNAL,\n"
                + "     CHECKSUM = DEFAULT,\n"
                + "     DEFAULT MERGEBLOCKRATIO,\n"
                + "     MAP = TD_MAP1\n"
                + "     (\n"
                + "      id INTEGER,\n"
                + "      name VARCHAR(30),\n"
                + "      age INTEGER,\n"
                + "      address VARCHAR(30) CHARACTER SET LATIN CASESPECIFIC,\n"
                + "      address1 VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,\n"
                + "      address2 VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC)\n"
                + "UNIQUE PRIMARY INDEX ( id );";

        String str2 = "create multiset table database_teradata1.table_1 as (select id,name,age,address from database_teradata1.table1) with data";

        String str3 = "create multiset table database_teradata1.table2 as (select id,name,age,address from database_teradata1.table1) with data;";

        String str4 = "create multiset table database_teradata1.table3 as (select id,name,age,address from database_teradata1.table2) with data;";

        String str5 = "create view database_teradata1.view1 as select id,name,age,address from database_teradata1.table1;";

        assertNotNull(sqlParser.getTargetSourceTableMap(str0));
        assertNotNull(sqlParser.getTargetSourceTableMap(str1));
        assertNotNull(sqlParser.getTargetSourceTableMap(str2));
        assertNotNull(sqlParser.getTargetSourceTableMap(str3));
        assertNotNull(sqlParser.getTargetSourceTableMap(str4));
        assertNotNull(sqlParser.getTargetSourceTableMap(str5));

    }

    @Test
    public void mysqlParser() {
        ISqlParser mySqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.MySQL);
        String dbType = "mysql";
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
        SQLUtils.format(mysql1, dbType);
        SQLUtils.format(mysql2, dbType);
        SQLUtils.format(mysql3, dbType);
        SQLUtils.format(mysql4, dbType);
        SQLUtils.format(mysql5, dbType);
        SQLUtils.format(mysql6, dbType);
        SQLUtils.format(mysql7, dbType);
        SQLUtils.format(mysql8, dbType);
        SQLUtils.format(mysql9, dbType);

        assertNotNull(SQLUtils.parseStatements(mysql1, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql2, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql3, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql4, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql5, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql6, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql7, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql8, dbType));
        assertNotNull(SQLUtils.parseStatements(mysql9, dbType));

        assertNotNull(mySqlParser.getTargetSourceTableMap(mysql2));
        assertNotNull(mySqlParser.getTargetSourceTableMap(mysql5));
        assertNotNull(mySqlParser.getTargetSourceTableMap(mysql6));
        assertNotNull(mySqlParser.getTargetSourceTableMap(mysql7));

    }

    @Test
    public void oracleParser() {
        String dbType = "oracle";
        ISqlParser oracleSqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.Oracle);
        String oracleSql1 = "create table STUDENT\n" +
                "(\n" +
                " ID NUMBER(10) not null\n" +
                " primary key,\n" +
                " NAME VARCHAR2(10) not null,\n" +
                " SEX VARCHAR2(4) default '男'\n" +
                " check (sex in('男','女'))\n" +
                ");";

        String oracleSql2 = "create table test1.table1(id number, name varchar2(10), age number, address varchar2(10));";

        String oracleSql3 = "create table test1.table2 as select id,name,age,address from test1.table1;";

        String oracleSql4 = "create table test1.table3 as select id,name,age,address from test1.table2;";

        String oracleSql5 = "CREATE VIEW TEST1.TEST_VIEW AS SELECT * FROM TEST1.STUDENT;";

        String oracleSql6 =
                "create or replace procedure test_procedure(a Date, b VARCHAR2, c INT) is\n" +
                        " x int;\n" +
                        "begin\n" +
                        " x := 123;\n" +
                        "end;";

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

        SQLUtils.format(oracleSql1, dbType);
        SQLUtils.format(oracleSql2, dbType);
        SQLUtils.format(oracleSql3, dbType);
        SQLUtils.format(oracleSql4, dbType);
        SQLUtils.format(oracleSql5, dbType);
        SQLUtils.format(oracleSql6, dbType);
        SQLUtils.format(oracleSql7, dbType);

        assertNotNull(SQLUtils.parseStatements(oracleSql1, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql2, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql3, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql4, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql5, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql6, dbType));
        assertNotNull(SQLUtils.parseStatements(oracleSql7, dbType));

        assertNotNull(oracleSqlParser
                .getTargetSourceTableMap(oracleSql1));
        assertNotNull(oracleSqlParser
                .getTargetSourceTableMap(oracleSql2));
        assertNotNull(oracleSqlParser
                .getTargetSourceTableMap(oracleSql3));
        assertNotNull(oracleSqlParser
                .getTargetSourceTableMap(oracleSql4));
        assertNotNull(oracleSqlParser
                .getTargetSourceTableMap(oracleSql5));

    }

    @Test
    public void db2Parser() {
        ISqlParser db2SqlParser = SqlParserFactory.getInstance()
                .createSqlParser(SqlParseType.DB2);
        String dbType = "db2";
        String db2Sql1 = "create table table1(id bigint, name varchar(20), age int, address varchar(50))";
        String db2FormatSql1 = SQLUtils.format(db2Sql1, "db2");

        String db2Sql2 = "CREATE TABLE test1.table2 LIKE test1.table1";
        String db2FormatSql2 = SQLUtils.format(db2Sql2, "db2");

        String db2Sql3 = "CREATE VIEW test1.view1 AS (SELECT id,name,age,address FROM test1.table1)";
        String db2FormatSql3 = SQLUtils.format(db2Sql3, "db2");

        String db2Sql4 = "create table table2 as (select id,name,age,address from table1) definition only";
        String db2FormatSql4 = SQLUtils.format(db2Sql4, "db2");

        String db2Sql5 = "create table table3 as (select * from table2) definition only";
        String db2FormatSql5 = SQLUtils.format(db2Sql5, "db2");

        String db2Sql6 = "CREATE OR REPLACE PROCEDURE INSERT_TABLE1 (IN in_id INTEGER,IN in_name VARCHAR(20)) BEGIN END";
        String db2FormatSql6 = SQLUtils.format(db2Sql6, "db2");

        String db2Sql7 = "insert into table1(id,name,age,address) values(1, 'mark', 18, 'shanghai')";
        String db2FormatSql7 = SQLUtils.format(db2Sql7, "db2");

        assertNotNull(SQLUtils.parseStatements(db2FormatSql1, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql2, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql3, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql4, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql5, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql6, dbType));
        assertNotNull(SQLUtils.parseStatements(db2FormatSql7, dbType));

        assertNotNull(db2SqlParser
                .getTargetSourceTableMap(db2Sql1));
        assertNotNull(db2SqlParser
                .getTargetSourceTableMap(db2Sql2));
        assertNotNull(db2SqlParser
                .getTargetSourceTableMap(db2Sql3));
        assertNotNull(db2SqlParser
                .getTargetSourceTableMap(db2Sql4));
        assertNotNull(db2SqlParser
                .getTargetSourceTableMap(db2Sql5));


    }

    @Test
    public void sqlServerParser() {
        String dbType = "sqlserver";
        String sqlServer1 = "create database test1";
        String sqlServer2 = "use test1";
        // 创建登录账户
        String sqlServer3 = "create login user1 with passworddddd='d'";
        // 为登录账户创建数据库用户
        String sqlServer4 = "create user user1 for login user1";
        // 删除数据库用户
        String sqlServer5 = "drop user user1";
        // 删除登录账户
        String sqlServer6 = "drop login user1";
        // 创建schema
        String sqlServer7 = "create schema test1";
        String sqlServer8 = "create table test1.table1\n" +
                "(\n" +
                "id int,\n" +
                "name varchar(20),\n" +
                "age int,\n" +
                "address varchar(50)\n" +
                ")";
        String sqlServer9 = "drop table test1.table2\n";
        String sqlServer10 = "insert into test1.table1 values(3,'lucy',22,'shenzheng')\n";
        String sqlServer11 = "select * into test1.table2 from test1.table1\n";
        String sqlServer12 = "insert into test1.table2 select * from test1.table1\n";
        String sqlServer13 = "select * into test1.table3 from test1.table2\n";
        String sqlServer14 = "create view test1.view1 as select * from test1.table1\n";
        String sqlServer15 = "create view test1.view2 as select * from test1.view1\n";
        String sqlServer16 = "create view test1.view3 as select * from test1.view2\n";
        String sqlServer17 = "drop view test1.view3";
        String sqlServer18 = "create procedure test1.procedure1\n" +
                "@id int\n" +
                "as\n" +
                "  begin\n" +
                "    insert into test1.table1 values(@id,'procedure1_name',@id,'procedure1_address')\n" +
                "  end";

        String sqlServer18_1 = "execute test1.procedure1 10\n";
        String sqlServer18_2 = "drop procedure test1.procedure1\n";

        String sqlServer19 = "create function test1.function1(@id int)\n" +
                "returns table\n" +
                "as \n" +
                "return select * from test1.table1 where id=@id";

        String sqlServer20 = "select * from test1.function1(1)\n";

        String sqlServer21 = "drop function test1.function1\n";

        SQLUtils.format(sqlServer1, dbType);

        SQLUtils.format(sqlServer2, dbType);

        SQLUtils.format(sqlServer3, dbType);

        SQLUtils.format(sqlServer4, dbType);

        SQLUtils.format(sqlServer5, dbType);

        SQLUtils.format(sqlServer6, dbType);

        SQLUtils.format(sqlServer7, dbType);

        SQLUtils.format(sqlServer8, dbType);

        SQLUtils.format(sqlServer9, dbType);

        SQLUtils.format(sqlServer10, dbType);

        SQLUtils.format(sqlServer11, dbType);

        SQLUtils.format(sqlServer12, dbType);

        SQLUtils.format(sqlServer13, dbType);

        SQLUtils.format(sqlServer14, dbType);

        SQLUtils.format(sqlServer15, dbType);

        SQLUtils.format(sqlServer16, dbType);

        SQLUtils.format(sqlServer17, dbType);

        SQLUtils.format(sqlServer18, dbType);

        SQLUtils.format(sqlServer18_1, dbType);

        SQLUtils.format(sqlServer18_2, dbType);

        SQLUtils.format(sqlServer19, dbType);

        SQLUtils.format(sqlServer20, dbType);

        SQLUtils.format(sqlServer21, dbType);

    }

    @Test
    public void teradataParser() {
        String dbType = "teradata";
        String str1 = "create database test2 as perm=200000000,spool=100000000;";
        String str2 = "select * from dbc.dbcinfo;";
        String str3 = "create multiset table test1.table1(id integer, name varchar(30), age integer, address varchar(30)) unique primary index(id);\n";
        String str4 = "create multiset table test1.table2(id decimal(10,3), name varchar(30), age integer, address varchar(30)) unique primary index(id);\n";
        String str5 = "insert into test1.table1(id,name,age,address) values (1,'jack',18,'shanghai');\n";
        String str6 = "insert into test1.table1(id,name,age,address) values (2,'lucy',19,'beijing');\n";
        String str7 = "insert into test1.table1(id,name,age,address) values (3,'mark',20,'guangzhou');\n";
        String str8 = "insert into test1.table1(id,name,age,address) values (4,'cici',20,'hangzhou');\n";
        String str9 = "create multiset table test1.table2 as (select id,name,age,address from test1.table1) with data;\n";
        String str10 = "create multiset table test1.table3 as (select id,name,age,address from test1.table2) with data;\n";
        String str11 = "select * from test1.table1;\n";
        String str12 = "COLLECT STATISTICS COLUMN(name) ON test1.table1;\n";
        String str13 = "HELP STATISTICS test1.table1; \n";
        String str14 = "explain select * from test1.table1;\n";
        String str15 = "select * from test1.table1 sample 3;\n";
        String str16 = "create view test1.view1 as select id,name,age,address from test1.table1;\n";
        String str17 = "select * from test1.view1;\n";
        String str18 = "GRANT CREATE PROCEDURE ON test1 TO test1;";
        String str19 = "CREATE PROCEDURE test1.insert_table1(\n" +
                "IN in_id INTEGER,\n" +
                "IN in_name VARCHAR(30),\n" +
                "IN in_age INTEGER,\n" +
                "IN in_address VARCHAR(30)\n" +
                ")\n" +
                "BEGIN \n" +
                "INSERT INTO test1.table1(\n" +
                "id,\n" +
                "name,\n" +
                "age,\n" +
                "address\n" +
                ") \n" +
                "VALUES( \n" +
                ":in_id, \n" +
                ":in_name, \n" +
                ":in_age,\n" +
                ":in_address \n" +
                ");\n" +
                "END;";

        String str20 = "CREATE PROCEDURE test1.insert_table2(\n" +
                "IN in_id INTEGER,\n" +
                "IN in_name VARCHAR(30),\n" +
                "IN in_age INTEGER\n" +
                ")\n" +
                "BEGIN \n" +
                "INSERT INTO test1.table1(\n" +
                "id,\n" +
                "name,\n" +
                "age\n" +
                ") \n" +
                "VALUES( \n" +
                ":in_id, \n" +
                ":in_name, \n" +
                ":in_age\n" +
                ");\n" +
                "END;\n";

        String str21 = "CREATE PROCEDURE test1.insert_table2(\n" +
                "IN in_id INTEGER,\n" +
                "IN in_name VARCHAR(30)\n" +
                ")\n" +
                "BEGIN \n" +
                "INSERT INTO test1.table1(\n" +
                "id,\n" +
                "name\n" +
                ") \n" +
                "VALUES( \n" +
                ":in_id, \n" +
                ":in_name\n" +
                ");\n" +
                "END;\n";

        String str22 = "DROP PROCEDURE test1.insert_table1;\n";
        String str22_2 = "CALL test1.insert_table1(5,'jame',22,'suzhou');\n";
        String str22_3 = "CALL test1.insert_table2(6,'hack',22);\n";
        String str22_4 = "select * from test1.view1;\n";
        String str22_5 = "GRANT CREATE FUNCTION ON test1 TO test1;\n";
        String str23 =
                "REPLACE FUNCTION test1.timestampdiff_char19(endtime VARCHAR(19),starttime VARCHAR(19))\n" +
                        "RETURNS INT\n" +
                        "LANGUAGE SQL\n" +
                        "CONTAINS SQL\n" +
                        "DETERMINISTIC\n" +
                        "SQL SECURITY DEFINER\n" +
                        "COLLATION INVOKER\n" +
                        "INLINE TYPE 1\n" +
                        "RETURN ( CAST(CAST(endtime as TIMESTAMP(0)) AS DATE FORMAT 'YYYYMMDD')-CAST(CAST(starttime as TIMESTAMP(0)) AS DATE FORMAT 'YYYYMMDD'))*24*3600\n"
                        +
                        "+EXTRACT(HOUR FROM (TO_TIMESTAMP(endtime,'YYYY-MM-DD HH24:MI:SS' )-TO_TIMESTAMP(SUBSTR(endtime,1,10)||' '||SUBSTR(starttime,12,8),'YYYY-MM-DD HH24:MI:SS' ) HOUR TO SECOND))*3600\n"
                        +
                        "+EXTRACT(MINUTE FROM (TO_TIMESTAMP(endtime,'YYYY-MM-DD HH24:MI:SS' )-TO_TIMESTAMP(SUBSTR(endtime,1,10)||' '||SUBSTR(starttime,12,8),'YYYY-MM-DD HH24:MI:SS' ) HOUR TO SECOND))*60\n"
                        +
                        "+EXTRACT(SECOND FROM (TO_TIMESTAMP(endtime,'YYYY-MM-DD HH24:MI:SS' )-TO_TIMESTAMP(SUBSTR(endtime,1,10)||' '||SUBSTR(starttime,12,8),'YYYY-MM-DD HH24:MI:SS' ) HOUR TO SECOND);\n";

        String str24 = "select id,name,age,address,test1.timestampdiff_char19('2019-02-14 23:23:23', '2019-01-01 11:11:11') as timestamp_diff_seconds from test1.table1;\n";
        String str25 = "select * from dbc.allrights where username='test1'; \n";
        String str26 = "select databasename from dbc.allrights where username='test1' group by databasename;\n";
        String str27 = "select * from dbc.databases where DBC.Databases.DatabaseName='test1';\n";
        String str28 = "select tablename from dbc.allrights where username='test1' and databasename='test1' group by tablename;\n";
        String str29 = "select * from dbc.allrights;\n";
        String str30 = "select * from dbc.dbase;\n";
        String str31 = "select * from dbc.tables where databasename='test1';\n";
        String str32 = "select tablename from dbc.tables where tablekind='T' and databasename='test1' and tablename in (select tablename from dbc.allrights where username='test1' and databasename='test1' group by tablename);\n";
        String str33 = "select * from dbc.tables where tablekind='V' and databasename='test1';\n";
        String str34 = "select * from dbc.tables where tablekind='P' and databasename='test1';\n";
        String str35 = "select * from dbc.tables where databasename='test1' and tablename='view1';\n";
        String str36 = "select * from dbc.functionsx where databasename='test1';\n";
        String str37 = "show procedure test1.insert_table1;\n";
        String str38 = "help procedure test1.insert_table1;\n";
        String str39 = "help procedure test1.insert_table1 attributes;\n";
        String str40 = "show table test1.table1;\n";
        String str41 = "help table test1.table1;\n";
        String str42 = "Select columnname,trim(columntitle) from dbc.columns Where databasename='数据库名' and tablename='表名' Order by columnid; \n";
        String str43 = "select * from dbc.accountinfo;\n";
        String str44 = "help table test1.table2;\n";
        String str45 = "help table test1.table1;\n";
        String str46 = "help table test1.view1;\n";
        String str47 = "help column test1.table1.id;\n";
        String str48 = "help column test1.view1.id;\n";
        String str49 = "select * from dbc.columns where database='test1' and tablename='view1';\n";
        String str50 = "select * from dbc.columnstatsv where database='test1';\n";
        String str51 = "select * from test1.table1 sample 20;\n";
        String str52 = "alter table test1.table1 add address1 varchar(30);\n";

        SQLUtils.format(str1, dbType);

        SQLUtils.format(str2, dbType);

        SQLUtils.format(str3, dbType);

        SQLUtils.format(str4, dbType);

        SQLUtils.format(str5, dbType);

        SQLUtils.format(str6, dbType);

        SQLUtils.format(str7, dbType);

        SQLUtils.format(str8, dbType);

        SQLUtils.format(str9, dbType);

        SQLUtils.format(str10, dbType);

        SQLUtils.format(str11, dbType);

        SQLUtils.format(str12, dbType);

        SQLUtils.format(str13, dbType);

        SQLUtils.format(str14, dbType);

        SQLUtils.format(str15, dbType);

        SQLUtils.format(str16, dbType);

        SQLUtils.format(str17, dbType);

        SQLUtils.format(str18, dbType);

        SQLUtils.format(str19, dbType);

        SQLUtils.format(str20, dbType);

        SQLUtils.format(str21, dbType);

        SQLUtils.format(str22, dbType);

        SQLUtils.format(str22_2, dbType);

        SQLUtils.format(str22_3, dbType);

        SQLUtils.format(str22_4, dbType);

        SQLUtils.format(str22_5, dbType);

        SQLUtils.format(str23, dbType);

        SQLUtils.format(str24, dbType);

        SQLUtils.format(str25, dbType);

        SQLUtils.format(str26, dbType);

        SQLUtils.format(str27, dbType);

        SQLUtils.format(str28, dbType);

        SQLUtils.format(str29, dbType);

        SQLUtils.format(str30, dbType);

        SQLUtils.format(str31, dbType);

        SQLUtils.format(str32, dbType);

        SQLUtils.format(str33, dbType);

        SQLUtils.format(str34, dbType);

        SQLUtils.format(str35, dbType);

        SQLUtils.format(str36, dbType);

        SQLUtils.format(str37, dbType);

        SQLUtils.format(str38, dbType);

        SQLUtils.format(str39, dbType);

        SQLUtils.format(str40, dbType);

        SQLUtils.format(str41, dbType);

        SQLUtils.format(str42, dbType);

        SQLUtils.format(str43, dbType);

        SQLUtils.format(str44, dbType);

        SQLUtils.format(str45, dbType);

        SQLUtils.format(str46, dbType);

        SQLUtils.format(str47, dbType);

        SQLUtils.format(str48, dbType);

        SQLUtils.format(str49, dbType);

        SQLUtils.format(str50, dbType);

        SQLUtils.format(str51, dbType);

        SQLUtils.format(str52, dbType);


    }

} 
