package pipes;

import java.util.Map;

public abstract class PipesContextLoader {
    public abstract PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException;
}
