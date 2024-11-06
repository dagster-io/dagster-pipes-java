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
import types.PipesMetadataValue;
import types.RawValue;
import types.Type;

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
    private Object payload;

    private Map<String, PipesMetadata> metadata = null;

    //Related to reportAssetMaterialization
    private boolean materialization = false;
    // TODO:: remove?
    //  private Map<String, PipesMetadataValue> materializationMetadata;
    private String dataVersion;
    private String materializationAssetKey;

    //Related to reportAssetCheck
    private boolean check = false;
    private String checkName;
    private boolean passed;
    // TODO:: remove?
    //  private Map<String, PipesMetadataValue> checkMetadata;
    private String checkAssetKey;

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

    void setPayload(Object payload) {
        this.payload = payload;
    }

    void setMaterialization(
        Map<String, PipesMetadataValue> metadata, String dataVersion, String assetKey
    ) {
        this.materialization = true;
        //this.materializationMetadata = metadata;
        this.dataVersion = dataVersion;
        this.materializationAssetKey = assetKey;
    }

    void setCheck(
        String checkName, boolean passed, Map<String, PipesMetadataValue> metadata, String assetKey
    ) {
        this.check = true;
        this.checkName = checkName;
        this.passed = passed;
        //this.checkMetadata = metadata;
        this.checkAssetKey = assetKey;
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
        try (PipesSession session = new PipesSession(pipesContext)) {
            session.openDagsterPipes(paramsLoader, contextLoader, messageWriter);
            System.out.println("Opened dagster pipes with set params.");
            if (this.payload != null) {
                session.getContext().reportCustomMessage(this.payload);
                System.out.println("Payload reported with custom message.");
            }

            if (this.materialization) {
                buildTestMetadata();
                session.getContext().reportAssetMaterialization(
                    this.metadata, this.dataVersion, this.materializationAssetKey
                );
            }
            if (this.check) {
                buildTestMetadata();
                session.getContext().reportAssetCheck(
                    this.checkName, this.passed, this.metadata, this.checkAssetKey
                );
            }
            System.out.println("Finished try session");
        } catch (Exception exception) {
            pipesContext.reportException(exception);
            // TODO:: remove
            throw exception;
        }
    }

    public Map<String, PipesMetadata> buildTestMetadata() {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
            this.metadata.put("float", new PipesMetadata(0.1, Type.FLOAT));
            this.metadata.put("int", new PipesMetadata(1, Type.INT));
            this.metadata.put("text", new PipesMetadata("hello", Type.TEXT));
            this.metadata.put("notebook", new PipesMetadata("notebook.ipynb", Type.NOTEBOOK));
            this.metadata.put("path", new PipesMetadata("/dev/null", Type.PATH));
            this.metadata.put("md", new PipesMetadata("**markdown**", Type.MD));
            this.metadata.put("bool_true", new PipesMetadata(true, Type.BOOL));
            this.metadata.put("bool_false", new PipesMetadata(false, Type.BOOL));
            this.metadata.put("asset", new PipesMetadata(new String[]{"foo", "bar"}, Type.ASSET));
            this.metadata.put(
                "dagster_run",
                new PipesMetadata("db892d7f-0031-4747-973d-22e8b9095d9d", Type.DAGSTER_RUN)
            );
            this.metadata.put("job", new PipesMetadata("my_other_job", Type.JOB));
            this.metadata.put("null", new PipesMetadata(null, Type.NULL));
            this.metadata.put("url", new PipesMetadata("https://dagster.io", Type.URL));
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("foo", "bar");
            jsonMap.put("baz", 1);
            jsonMap.put("qux", new int[]{1, 2, 3});
            Map<String, Integer> inner = new HashMap<>();
            inner.put("a", 1);
            inner.put("b", 2);
            jsonMap.put("quux", inner);
            jsonMap.put("corge", null);
            this.metadata.put("json", new PipesMetadata(jsonMap, Type.JSON));
        }
        return this.metadata;
    }
}