package pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.InflaterInputStream;

public class PipesMappingParamsLoader implements PipesParamsLoader {
    private final String CONTEXT_ENV_VAR = "DAGSTER_PIPES_CONTEXT";
    private final String MESSAGES_ENV_VAR = "DAGSTER_PIPES_MESSAGES";
    private final Map<String, String> mapping;

    public static void main(String[] args) {
        PipesMappingParamsLoader loader = new PipesMappingParamsLoader(new HashMap<>());
        loader.mapping.put(loader.CONTEXT_ENV_VAR, "eJyrVipILMlQslJQ0i/JLQDh7PTywkqTwmz95Py8ktSKEqVaAOUDDP8=");
        loader.mapping.put(loader.MESSAGES_ENV_VAR, "eJyrVipILMlQslJQ0i/JLQDhrCpzs8qCDCP93NTi4sT01GKlWgDruAzj");
        System.out.println(loader.loadContextParams());
        System.out.println(loader.loadMessagesParams());
    }

    public PipesMappingParamsLoader(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    public boolean isDagsterPipesProcess() {
        return this.mapping.containsKey(CONTEXT_ENV_VAR);
    }

    public Map<String, String> loadContextParams() {
        String rawValue = this.mapping.get(CONTEXT_ENV_VAR);
        return decodeParam(rawValue);
    }

    public Map<String, String> loadMessagesParams() {
        String rawValue = this.mapping.get(MESSAGES_ENV_VAR);
        return decodeParam(rawValue);
    }

    private Map<String, String> decodeParam(String rawValue) {
        try {
            byte[] base64Decoded = Base64.getDecoder().decode(rawValue);
            byte[] zlibDecompressed = zlibDecompress(base64Decoded);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(
                    zlibDecompressed,
                    new TypeReference<Map<String, String>>() {}
            );
        } catch (IOException ioe) {
            // TODO: Add logging here, if needed
            throw new RuntimeException("Failed to decompress parameters", ioe);
        }
    }

    private byte[] zlibDecompress(byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
             InflaterInputStream filterStream = new InflaterInputStream(inputStream);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int readChunk;

            while ((readChunk = filterStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, readChunk);
            }

            return outputStream.toByteArray();
        }
    }
}
