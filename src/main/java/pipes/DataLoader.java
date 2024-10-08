package pipes;

import java.util.Map;

public class DataLoader {

    public static PipesContextData getData() throws DagsterPipesException {
        PipesMappingParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesDefaultContextLoader contextLoader = new PipesDefaultContextLoader();
        return contextLoader.loadContext(paramsLoader.loadContextParams());
    }

    public static PipesContextData getData(Map<String, String> input) throws DagsterPipesException {
        PipesMappingParamsLoader paramsLoader = new PipesMappingParamsLoader(input);
        PipesDefaultContextLoader contextLoader = new PipesDefaultContextLoader();
        return contextLoader.loadContext(paramsLoader.loadContextParams());
    }
}
