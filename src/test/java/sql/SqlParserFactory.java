package sql;

import sql.druid.DruidSqlParser ;
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
//    ISqlParser oracleSqlParser = SqlParserFactory.getInstance().createSqlParser(SqlParseType.Oracle);
//
//    String oracleSql1 = "create table test1.table1(id number, name varchar2(10), age number, address varchar2(10));";
//
//    String oracleSql2 = "create table test1.table2 as select id,name,age,address from test1.table1;";
//
//    Map<String, List<String>> sourceTargetTableMapOracle1 = oracleSqlParser.getTargetSourceTableMap(oracleSql1);
//
//    Map<String, List<String>> sourceTargetTableMapOracle2 = oracleSqlParser.getTargetSourceTableMap(oracleSql2);

    ISqlParser db2SqlParser = SqlParserFactory.getInstance().createSqlParser(SqlParseType.DB2);

    String db2Sql1 = "create table table1(id bigint, name varchar(20), age int, address varchar(50))";

//    String db2Sql2 = "CREATE TABLE test1.table2";

    String db2Sql2 = "CREATE TABLE test1.table2 LIKE test1.table1";

//    String db2Sql3 = "CREATE VIEW test1.view1 AS (SELECT id,name,age,address FROM test1.table1)";

    String db2Sql4 = "create table table2 as (select id,name,age,address from table1) definition only";

//    String db2Sql4 = "create table table2 as (select id,name,age,address from table1)";

//    Map<String, List<String>> sourceTargetTableMapDB2_1 = db2SqlParser.getTargetSourceTableMap(db2Sql1);

//    Map<String, List<String>> sourceTargetTableMapDB2_2 = db2SqlParser.getTargetSourceTableMap(db2Sql2);

//    Map<String, List<String>> sourceTargetTableMapDB2_3 = db2SqlParser.getTargetSourceTableMap(db2Sql3);

    Map<String, List<String>> sourceTargetTableMapDB2_4 = db2SqlParser.getTargetSourceTableMap(db2Sql4);

    System.out.println("");

  }

}
