package pipes;

import java.util.Map;
import java.util.Optional;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();
    Optional<Map<String, Object>> loadContextParams();
    Optional<Map<String, Object>> loadMessagesParams();
}
