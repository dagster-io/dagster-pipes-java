package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pipes.data.PipesContextData;
import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesParamsLoader;
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
    private Map<String, Object> payload;

    void setInput(Map<String, String> input) {
        this.input = input;
    }

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
        this.jobName = jobName;
    }

    void setWriter(PipesMessageWriterChannel writer) {
        this.writer = writer;
    }

    void setWriter() throws DagsterPipesException {
        this.writer = WriterChannelLoader.getWriter(input);
    }

    void setContextData(PipesContextData contextData) {
        this.contextData = contextData;
    }

    void setContextData() throws DagsterPipesException {
        this.contextData = DataLoader.getData(input);
    }

    void setPayload(Map<String, Object> payload) {
        this.payload = payload;
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
    public void fullTest() throws DagsterPipesException, IOException {
        PipesParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesContextLoader contextLoader = new PipesDefaultContextLoader();
        PipesMessageWriter messageWriter = new PipesDefaultMessageWriter();
        PipesContext pipesContext = new PipesContext(paramsLoader, contextLoader, messageWriter);
        try (PipesSession session = new PipesSession(pipesContext)) {
            session.openDagsterPipes(paramsLoader, contextLoader, messageWriter);
            System.out.println("Opened dagster pipes with set params.");
            if (this.payload != null) {
                session.getContext().reportCustomMessage(this.payload);
                System.out.println("Payload reported with custom message.");
            }
        } catch (Exception exception) {
            pipesContext.reportException(exception);
        }
    }
}