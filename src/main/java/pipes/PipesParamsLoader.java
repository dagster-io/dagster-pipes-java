package pipes;

import java.util.Map;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();
    Map<String, String> loadContextParams();
    Map<String, String> loadMessagesParams();
}
