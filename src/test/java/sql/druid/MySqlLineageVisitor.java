package sql.druid;

import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlLineageVisitor extends MySqlSchemaStatVisitor {
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
    public boolean visit(MySqlCreateTableStatement x) {
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
    public void endVisit(MySqlCreateTableStatement x) {

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


}
