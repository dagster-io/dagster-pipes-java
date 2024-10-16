package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pipes.writers.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Disabled
public class PipesTests {

    private Map<String, String> input;
    private PipesContextData contextData;
    private PipesMessageWriterChannel writer;
    private Map<String, Object> extras;
    private String jobName;

    PipesTests(
        PipesContextData pipesContextData,
        PipesMessageWriterChannel writer
    ) throws DagsterPipesException {
        this.contextData = pipesContextData;
        this.writer = writer;
    }

    void setInput(Map<String, String> input) {
        this.input = input;
    }

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
       this.jobName = jobName;
    }

    @Test
    public void testExtras() {
        Assertions.assertTrue(
            contextData.getExtras().entrySet().containsAll(this.extras.entrySet()),
            "Extras does not contain all provided entries."
        );
        System.out.println("Extras are correct.");
    }

    @Test
    public void testJobName() {
        Assertions.assertEquals(
            this.jobName,
            contextData.getJobName(),
            "JobName is incorrect."
        );
        System.out.println("JobName is correct.");
    }

    @Test
    public void testMessageWriter() throws IOException {
        Map<String, Object> randomMap = new HashMap<>();
        randomMap.put("1", 1);
        randomMap.put("2", 2);
        PipesMessage message = new PipesMessage("1.0", "bla", randomMap);
        this.writer.writeMessage(message);
        if (this.writer instanceof PipesFileMessageWriterChannel) {
            File file = new File(((PipesFileMessageWriterChannel) this.writer).getPath());
            Assertions.assertTrue(file.exists());
            Assertions.assertEquals(
                "{\"dagsterPipesVersion\":\"1.0\",\"method\":\"bla\",\"params\":{\"1\":1,\"2\":2}}",
                TestUtils.getLastLine(file.getPath())
            );
        }
    }

    @Test
    public void fullTest() throws Exception {
        PipesParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesContextLoader contextLoader = new PipesDefaultContextLoader();
        PipesMessageWriter messageWriter = new PipesDefaultMessageWriter();
        PipesContext pipesContext = new PipesContext(paramsLoader, contextLoader, messageWriter);
        try {
            // TODO::
        } catch (Exception exception) {
            pipesContext.reportException(exception);
        } finally {
            pipesContext.close();
        }
    }
}