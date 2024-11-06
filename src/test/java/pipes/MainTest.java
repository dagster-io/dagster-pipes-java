package pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import pipes.data.PipesConstants;
import types.PipesMetadataValue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@CommandLine.Command(name = "main-test", mixinStandardHelpOptions = true)
public class MainTest implements Runnable {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, Object> cachedJson = new ConcurrentHashMap<>();

    @CommandLine.Option(
        names = {"--context"},
        description = "Provide DAGSTER_PIPES_CONTEXT value for testing"
    )
    private String context;

    @CommandLine.Option(
        names = {"--messages"},
        description = "Provide DAGSTER_PIPES_MESSAGES value for testing"
    )
    private String messages;

    @CommandLine.Option(
        names = {"--env"},
        description = "Get DAGSTER_PIPES_MESSAGES & DAGSTER_PIPES_CONTEXT values " +
            "from environmental variables"
    )
    private boolean env = false;

    @CommandLine.Option(
        names = {"--jobName"},
        description = "Provide value of 'jobName' for testing"
    )
    private String jobName;

    @CommandLine.Option(
        names = {"--extras"},
        description = "Provide path to 'extras' JSON for testing"
    )
    private String extras;

    @CommandLine.Option(
        names = {"--full"},
        description = "Flag to test full PipesContext usage"
    )
    private boolean full = false;

    @CommandLine.Option(
        names = {"--custom-payload-path"},
        description = "Specify custom payload path"
    )
    private String customPayloadPath;

    @CommandLine.Option(
        names = {"--report-asset-check"},
        description = "Specify path to JSON with parameters to test reportAssetCheck"
    )
    private String reportAssetCheckJson;

    @CommandLine.Option(
        names = {"--report-asset-materialization"},
        description = "Specify path to JSON with parameters to test reportAssetMaterialization"
    )
    private String reportAssetMaterializationJson;

    @Override
    public void run() {
        Map<String, String> input = new HashMap<>();
        PipesTests pipesTests = new PipesTests();
        try {
            if (this.context != null) {
                input.put(PipesConstants.CONTEXT_ENV_VAR.name, this.context);
            }
            if (this.messages != null) {
                input.put(PipesConstants.MESSAGES_ENV_VAR.name, this.messages);
            }
            pipesTests.setInput(input);

            if (this.customPayloadPath != null && !this.customPayloadPath.isEmpty()) {
                cacheJson(this.customPayloadPath);
                Object payload = loadParamByWrapperKey("payload", Object.class);
                pipesTests.setPayload(payload);
            }

            if (this.reportAssetMaterializationJson != null && !this.reportAssetMaterializationJson.isEmpty()) {
                cacheJson(this.reportAssetMaterializationJson);
                Map<String, PipesMetadataValue> metadata = loadParamByWrapperKey("metadata", Map.class);
                String dataVersion = loadParamByWrapperKey("dataVersion", String.class);
                String assetKey = loadParamByWrapperKey("assetKey", String.class);
                pipesTests.setMaterialization(metadata, dataVersion, assetKey);
            }

            if (this.reportAssetCheckJson != null && !this.reportAssetCheckJson.isEmpty()) {
                cacheJson(this.reportAssetCheckJson);
                String checkName = loadParamByWrapperKey("checkName", String.class);
                boolean passed = loadParamByWrapperKey("passed", Boolean.class);
                Map<String, PipesMetadataValue> metadata = loadParamByWrapperKey("metadata", Map.class);
                String assetKey = loadParamByWrapperKey("assetKey", String.class);
                pipesTests.setCheck(checkName, passed, metadata, assetKey);
            }

            if (this.full) {
                pipesTests.fullTest();
                return;
            } else {
                pipesTests.setContextData();
                pipesTests.setWriter();
            }

            if (this.extras != null) {
                File jsonFile = new File(this.extras);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> extrasMap = objectMapper.readValue(
                    jsonFile, new TypeReference<Map<String, Object>>() {}
                );
                pipesTests.setExtras(extrasMap);
                pipesTests.testExtras();
            }

            if (this.jobName != null) {
                pipesTests.setJobName(this.jobName);
                pipesTests.testJobName();
            }

            //TODO:: delete or modify the test
            //pipesTests.testMessageWriter();
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }

        System.out.println("All tests finished.");
    }

    private void cacheJson(String jsonFilePath) {
        try {
            File jsonFile = new File(jsonFilePath);
            this.cachedJson = this.objectMapper.readValue(jsonFile, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JSON from file: " + jsonFilePath, e);
        }
    }

    private <T> T loadParamByWrapperKey(String wrapperKey, Class<T> type) {
        Object object = this.cachedJson.get(wrapperKey);
        if (object != null && !type.isInstance(object)) {
            throw new IllegalArgumentException(
                String.format(
                    "Wrong type for %s parameter. Expected: %s, found: %s",
                    wrapperKey, type.getTypeName(), object.getClass().getTypeName()
                )
            );
        } else {
            return (T) object;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MainTest()).execute(args);
        System.exit(exitCode);
    }
}