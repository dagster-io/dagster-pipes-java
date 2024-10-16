package pipes;

import java.util.Map;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();
    Map<String, Object> loadContextParams();
    Map<String, Object> loadMessagesParams();
}
