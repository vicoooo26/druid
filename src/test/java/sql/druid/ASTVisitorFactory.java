package sql.druid;

import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sql.SqlParseType;

public class ASTVisitorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ASTVisitorFactory.class);

    private static class Holder {

        private static final ASTVisitorFactory INSTANCE = new ASTVisitorFactory();
    }

    public static final ASTVisitorFactory getInstance() {
        return ASTVisitorFactory.Holder.INSTANCE;
    }

    private ASTVisitorFactory() {
    }

    public SchemaStatVisitor createSchemaStatVisitor(SqlParseType sqlParseType) {
        switch (sqlParseType) {
            case Hive:
            case Inceptor:
            case HBase:
            case Hyperbase:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
            case Oracle:
                return new OracleSqlLineageVisitor();
            case Teradata:
                return new TeradataLineageVisitor();
            case DB2:
                return new DB2LineageVisitor();
            case MySQL:
                return new MySqlLineageVisitor();
            case SqlServer:
                return new SQLServerLineageVisitor();
            default:
                throw new RuntimeException("not support " + sqlParseType.getType() + " yet.");
        }
    }
}
