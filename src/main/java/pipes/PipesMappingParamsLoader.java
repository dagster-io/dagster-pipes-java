package pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.zip.InflaterInputStream;

public class PipesMappingParamsLoader implements PipesParamsLoader {
    private final Map<String, String> mapping;

    public PipesMappingParamsLoader(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    public boolean isDagsterPipesProcess() {
        return this.mapping.containsKey(PipesConstants.CONTEXT_ENV_VAR.name);
    }

    public Optional<Map<String, Object>> loadContextParams() {
        String rawValue = this.mapping.get(PipesConstants.CONTEXT_ENV_VAR.name);
        if (rawValue == null) {
            System.out.printf(
                    "Provided mapping doesn't contain %s%n",
                    PipesConstants.CONTEXT_ENV_VAR.name
            );
            return Optional.empty();
        }
        return Optional.of(decodeParam(rawValue));
    }

    public Optional<Map<String, Object>> loadMessagesParams() {
        String rawValue = this.mapping.get(PipesConstants.MESSAGES_ENV_VAR.name);
        if (rawValue == null) {
            System.out.printf(
                    "Provided mapping doesn't contain %s%n",
                    PipesConstants.MESSAGES_ENV_VAR.name
            );
            return Optional.empty();
        }
        return Optional.of(decodeParam(rawValue));
    }

    private Map<String, Object> decodeParam(String rawValue) {
        try {
            byte[] base64Decoded = Base64.getDecoder().decode(rawValue);
            byte[] zlibDecompressed = zlibDecompress(base64Decoded);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(
                    zlibDecompressed,
                    new TypeReference<Map<String, Object>>() {}
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