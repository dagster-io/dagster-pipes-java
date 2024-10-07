package pipes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();
    Map<String, Object> loadContextParams();
    Map<String, Object> loadMessagesParams();
}
