package sql.druid;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Name;
import com.alibaba.druid.util.JdbcConstants;
import sql.ISqlParser;

import java.util.*;

public class DruidSqlParser implements ISqlParser {

  private String dbType;

  public DruidSqlParser(String dbType) {
    this.dbType = dbType;
  }

  public static Map<String, TreeSet<String>> getFromTo2(String sql) throws ParserException {
    List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
    if (stmts == null) {
      return null;
    }
    TreeSet<String> fromSet = new TreeSet<>();
    TreeSet<String> toSet = new TreeSet<>();

    String database = "DEFAULT";
    for (SQLStatement stmt : stmts) {
      SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.HIVE);
      if (stmt instanceof SQLUseStatement) {
        database = ((SQLUseStatement) stmt).getDatabase().getSimpleName().toUpperCase();
      }
      stmt.accept(statVisitor);
      Map<Name, TableStat> tables = statVisitor.getTables();
      if (tables != null) {
        final String db = database;
        tables.forEach((tableName, stat) -> {
          if (stat.getCreateCount() > 0 || stat.getInsertCount() > 0) {
            String to = tableName.getName().toUpperCase();
            if (!to.contains(".")) {
              to = db + "." + to;
            }
            toSet.add(to);
          } else if (stat.getSelectCount() > 0) {
            String from = tableName.getName().toUpperCase();
            if (!from.contains(".")) {
              from = db + "." + from;
            }
            fromSet.add(from);
          }
        });
      }
    }
    Map<String, TreeSet<String>> fromTo = new HashMap<>(4);
    fromTo.put("from", fromSet);
    fromTo.put("to", toSet);
    return fromTo;
  }

  private static Set<String> getFromTableFromTableSource(SQLTableSource sts) {
    Set<String> from = new HashSet<>();
    if (sts instanceof SQLJoinTableSource) {
      from = getFromTableFromJoinSource((SQLJoinTableSource) sts);
    } else {
      from.add(sts.toString().toUpperCase());
    }
    return from;
  }

  private static Set<String> getFromTableFromJoinSource(SQLJoinTableSource sjts) {
    Set<String> result = new HashSet<>();
    getFromTable(result, sjts);
    return result;
  }

  // 递归获取join的表list
  private static void getFromTable(Set<String> fromList, SQLJoinTableSource sjts) {
    SQLTableSource left = sjts.getLeft();
    if (left instanceof SQLJoinTableSource) {
      getFromTable(fromList, (SQLJoinTableSource) left);
    } else {
      fromList.add(left.toString().toUpperCase());
    }
    SQLTableSource right = sjts.getRight();
    if (right instanceof SQLJoinTableSource) {
      getFromTable(fromList, (SQLJoinTableSource) right);
    } else {
      fromList.add(right.toString().toUpperCase());
    }
  }

  @Override
  public Map<String, List<String>> getTargetSourceTableMap(String sql) {
    Map<String, List<String>> result = new HashMap<>();
    //format中其实也是通过parser来解析成ast，然后用一个SQLASTOutputVisitor来访问ast
//    String formatSql = SQLUtils.format(sql, dbType);
    List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
    stmtList.get(0).toString();
//    List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
    List<String> sourceNames = new ArrayList<>();
    List<String> targetNames = new ArrayList<>();
    for (int i = 0; i < stmtList.size(); i++) {
      sourceNames.clear();
      targetNames.clear();
      String database = null;
      SQLStatement stmt = stmtList.get(i);
      if (stmt instanceof SQLUseStatement) {
        database = ((SQLUseStatement) stmt).getDatabase().getSimpleName().toUpperCase();
      }
      SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(dbType);
      stmt.accept(visitor);
      Map<Name, TableStat> tables = visitor.getTables();
      if (null == tables) {
        continue;
      }
      for (Map.Entry<Name, TableStat> entry : tables.entrySet()) {
        final String tableName = entry.getKey().getName();
        TableStat tableStat = entry.getValue();
        if (tableStat.getCreateCount() > 0 || tableStat.getInsertCount() > 0) {
          if (!tableName.contains(".") && org.apache.commons.lang.StringUtils
              .isNotBlank(database)) {
            targetNames.add(database + "." + tableName);
          } else {
            targetNames.add(tableName);
          }
        }
        if (tableStat.getSelectCount() > 0) {
          if (!tableName.contains(".") && org.apache.commons.lang.StringUtils
              .isNotBlank(database)) {
            sourceNames.add(database + "." + tableName);
          } else {
            sourceNames.add(tableName);
          }
        }
      }

      if (sourceNames.isEmpty()) {
        List<String> tmp = new ArrayList<>();
        tmp.addAll(targetNames);
        result.put(null, tmp);
      } else {
        for (String sourceName : sourceNames) {
          if (!result.containsKey(sourceName)) {
            result.put(sourceName, new ArrayList<>());
          }
          result.get(sourceName).addAll(targetNames);
        }
      }
    }
    return result;
  }

}
