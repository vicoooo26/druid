package sql.druid;

import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2CreateRestriction;
import com.alibaba.druid.sql.dialect.db2.ast.DB2Object;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2CreateTableStatement;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2ValuesStatement;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB2LineageVisitor extends DB2SchemaStatVisitor {

    protected final Map<String, List<String>> lineage = new HashMap<>();

    public Map<String, List<String>> getLineage() {
        return lineage;
    }

    protected Map<String, List<String>> addLineage(String sourceName, String targetName) {
        Map<String, List<String>> lineage = this.getLineage();
        List<String> targets = new ArrayList<>();
        if (null == lineage.get(sourceName)) {
            targets.add(targetName);
            lineage.put(sourceName, targets);
        } else {
            targets = lineage.get(sourceName);
            targets.add(targetName);
            lineage.put(sourceName, targets);
        }
        return lineage;
    }

    @Override
    public boolean visit(DB2SelectQueryBlock x) {
        return this.visit((SQLSelectQueryBlock) x);
    }

    @Override
    public void endVisit(DB2SelectQueryBlock x) {
        super.endVisit((SQLSelectQueryBlock) x);
    }

    @Override
    public boolean visit(DB2ValuesStatement x) {
        return false;
    }

    @Override
    public void endVisit(DB2ValuesStatement x) {

    }

    @Override
    public boolean visit(DB2CreateTableStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        for (SQLTableElement e : x.getTableElementList()) {
            e.setParent(x);
        }

        TableStat stat = getTableStat(x.getName());
        stat.incrementCreateCount();

        accept(x.getTableElementList());

        if (x.getInherits() != null) {
            x.getInherits().accept(this);
        }

        if (x.getSelect() != null) {
            x.getSelect().accept(this);
        }

        // TODO 多层父子关系嵌套的血缘
        StringBuilder sourceName = new StringBuilder();
        sourceName.append("");
        if (null != x.getLike()) {
            sourceName.append(x.getLike().toString());
        } else if (null != x.getSelect()) {
            sourceName.append(((SQLSelectQueryBlock) x.getSelect().getQuery()).getFrom());
        }
        String targetName = x.getName().toString();
        addLineage(sourceName.toString(), targetName);
        return false;
    }

    @Override
    public void endVisit(DB2CreateTableStatement x) {

    }

    @Override
    public boolean visit(SQLCreateViewStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        x.getSubQuery().accept(this);
        StringBuilder sourceName = new StringBuilder();
        sourceName.append("");
        if (null != x.getSubQuery()) {
            sourceName.append(((SQLSelectQueryBlock) x.getSubQuery().getQuery()).getFrom());
        }
        String targetName = x.getName().toString();
        addLineage(sourceName.toString(), targetName);
        return false;

    }

    @Override
    public void endVisit(SQLCreateViewStatement x) {

    }


    @Override
    public boolean visit(DB2CreateRestriction.DefinitionOnly x) {
        return true;
    }

    @Override
    public void endVisit(DB2CreateRestriction.DefinitionOnly x) {

    }


    protected boolean isPseudoColumn(long hash64) {
        return hash64 == DB2Object.Constants.CURRENT_DATE
                || hash64 == DB2Object.Constants.CURRENT_DATE2
                || hash64 == DB2Object.Constants.CURRENT_TIME
                || hash64 == DB2Object.Constants.CURRENT_SCHEMA;
    }


}
