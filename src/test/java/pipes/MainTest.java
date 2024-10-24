package pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import pipes.data.PipesConstants;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@CommandLine.Command(name = "main-test", mixinStandardHelpOptions = true)
public class MainTest implements Runnable {

    private final ObjectMapper objectMapper = new ObjectMapper();

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
        names = {"--customPayloadPath"},
        description = "Specify custom payload path"
    )
    private String customPayloadPath;

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
                Map<String, Object> payload = loadPayload(this.customPayloadPath);
                pipesTests.setPayload(payload);
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
                Map<String, Object> extrasMap = objectMapper.readValue(jsonFile, new TypeReference<>(){});
                pipesTests.setExtras(extrasMap);
                pipesTests.testExtras();
            }

            if (this.jobName != null) {
                pipesTests.setJobName(this.jobName);
                pipesTests.testJobName();
            }

            //TODO:: delete or modify the test
            //pipesTests.testMessageWriter();
        } catch (DagsterPipesException | IOException exception) {
            throw new RuntimeException(exception);
        }

        System.out.println("All tests finished.");
    }

    private Map<String, Object> loadPayload(String jsonFilePath) {
        File jsonFile = new File(jsonFilePath);
        try {
            return this.objectMapper.readValue(jsonFile, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JSON from file: " + jsonFilePath, e);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MainTest()).execute(args);
        System.exit(exitCode);
    }
}