package sql;

public enum SqlParseType {

    Hive("hive", "org.apache.hive.jdbc.HiveDriver"), //NOSONAR

    Inceptor("inceptor", ""), //NOSONAR

    HBase("hbase", ""), //NOSONAR

    Hyperbase("hyperbase", ""), //NOSONAR

    Oracle("oracle", "oracle.jdbc.OracleDriver"), //NOSONAR

    DB2("db2", "com.ibm.db2.jcc.DB2Driver"), //NOSONAR

    Teradata("teradata", "com.teradata.jdbc.TeraDriver"), //NOSONAR

    MySQL("mysql", "com.mysql.jdbc.Driver"), //NOSONAR

    SqlServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"); //NOSONAR

    private String type;

    private String driver;

    SqlParseType(String type, String driver) {
        this.type = type;
        this.driver = driver;
    }

    public String getType() {
        return type;
    }

    public String getDriver() {
        return driver;
    }

    public static SqlParseType typeOf(String dbType) {
        for (SqlParseType sqlParseType : SqlParseType.values()) {
            if (sqlParseType.getType().equalsIgnoreCase(dbType)) {
                return sqlParseType;
            }
        }
        throw new IllegalArgumentException(
                "No enum constant " + dbType);
    }
}
