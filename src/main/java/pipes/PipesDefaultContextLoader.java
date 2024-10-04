package pipes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class PipesDefaultContextLoader {
    private final String FILE_PATH_KEY = "path";
    private final String DIRECT_KEY = "data";

    public PipesContextData loadContext(Map<String, String> params) {
        try {
            if (params.containsKey(FILE_PATH_KEY)) {
                String path = assertEnvParamType();
                return loadFromFile(path);
            } else if (params.containsKey(DIRECT_KEY)) {
                Map<String, String> data = assertParamType();
                return new PipesContextData(data);
            } else {
                //TODO:: change to DagsterException
                throw new RuntimeException(
                    String.format(
                        "Invalid params: expected key %s or %s",
                        FILE_PATH_KEY,
                        DIRECT_KEY
                    )
                );
            }
        }
    }

    private PipesContextData loadFromFile(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> data = mapper.readValue(new File(path), Map.class);
        return new PipesContextData(data);
    }


}
