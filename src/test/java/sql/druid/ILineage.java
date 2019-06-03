package sql.druid;

import java.util.List;
import java.util.Map;

public interface ILineage {
   Map<String, List<String>> getLineage();
}
