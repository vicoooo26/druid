package sql;

import java.util.List;
import java.util.Map;

public interface ISqlParser {

  /**
   * Map<TargetTable, List<SourceTable>>
   */
  Map<String, List<String>> getTargetSourceTableMap(String sql);

}
