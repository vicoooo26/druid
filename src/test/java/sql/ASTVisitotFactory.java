package sql;

import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sql.druid.DB2LineageVisitor;
import sql.druid.MySqlLineageVisitor;
import sql.druid.OracleSqlLineageVisitor;

public class ASTVisitotFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ASTVisitotFactory.class);

    private static class Holder {

        private static final ASTVisitotFactory INSTANCE = new ASTVisitotFactory();
    }

    public static final ASTVisitotFactory getInstance() {
        return ASTVisitotFactory.Holder.INSTANCE;
    }

    private ASTVisitotFactory() {
    }

    public SchemaStatVisitor createSchemaStatVisitor(SqlParseType sqlParseType) {
        switch (sqlParseType) {
            case Teradata:
            case SqlServer:
            case Hive:
            case Inceptor:
            case HBase:
            case Hyperbase:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
            case Oracle:
                return new OracleSqlLineageVisitor();
            case DB2:
                return new DB2LineageVisitor();
            case MySQL:
                return new MySqlLineageVisitor();
            default:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
        }
    }
}
