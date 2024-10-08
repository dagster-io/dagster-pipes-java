package pipes;

import picocli.CommandLine;

import java.util.HashMap;
import java.util.Map;

@CommandLine.Command(name = "main-test", mixinStandardHelpOptions = true)
public class MainTest implements Runnable {

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
        description = "Provide value of 'extras' for testing"
    )
    private Map<String, String> extras;

    @Override
    public void run() {
        Map<String, String> input = new HashMap<>();
        ContextDataTest contextDataTest;
        try {
            if (this.env) {
                contextDataTest = new ContextDataTest(DataLoader.getData());
            } else {
                if (this.context != null) {
                    input.put(PipesVariables.CONTEXT_ENV_VAR.name, this.context);
                }
                if (this.messages != null) {
                    input.put(PipesVariables.MESSAGES_ENV_VAR.name, this.messages);
                }
                contextDataTest = new ContextDataTest(DataLoader.getData(input));
            }

            if (this.extras != null) {
                contextDataTest.setExtras(this.extras);
                contextDataTest.testExtras();
            }

            if (this.jobName != null) {
                contextDataTest.setJobName(this.jobName);
                contextDataTest.testJobName();
            }
        } catch (DagsterPipesException dpe) {
            throw new RuntimeException(dpe);
        }

        System.out.println("All tests finished.");
    }


    public static void main(String[] args) {
        int exitCode = new CommandLine(new MainTest()).execute(args);
        System.exit(exitCode);
    }
}
