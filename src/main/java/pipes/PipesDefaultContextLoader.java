package pipes;

import com.fasterxml.jackson.databind.ObjectMapper;
import pipes.utils.PipesUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class PipesDefaultContextLoader {
    private final String FILE_PATH_KEY = "path";
    private final String DIRECT_KEY = "data";

    public PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException {
        if (params.containsKey(FILE_PATH_KEY)) {
            String path = PipesUtils.assertEnvParamType(
                params, FILE_PATH_KEY, String.class, PipesDefaultContextLoader.class
            );
            return loadFromFile(path);
        } else if (params.containsKey(DIRECT_KEY)) {
            Map<String, Object> data = PipesUtils.assertEnvParamType(
                params, DIRECT_KEY, Map.class, PipesDefaultContextLoader.class
            );
            ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(data, PipesContextData.class);
        } else {
            throw new DagsterPipesException(
                String.format(
                    "Invalid params: expected key %s or %s",
                    FILE_PATH_KEY,
                    DIRECT_KEY
                )
            );
        }
    }

    private PipesContextData loadFromFile(String path) throws DagsterPipesException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> data = mapper.readValue(new File(path), Map.class);
            return mapper.convertValue(data, PipesContextData.class);
        } catch (IOException ioe) {
            throw new DagsterPipesException(
                String.format(
                    "Failed to read context data from file: %s",
                    path
                ),
                ioe
            );
        }
    }
}